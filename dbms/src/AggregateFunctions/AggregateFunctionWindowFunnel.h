#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <common/logger_useful.h>
#include <unordered_set>

#include <sstream>
#include <iostream>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ArenaAllocator.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct ComparePairFirst final
{
    template <typename T1, typename T2>
    bool operator()(const std::pair<T1, T2> & lhs, const std::pair<T1, T2> & rhs) const
    {
        return lhs.first < rhs.first;
    }
};

struct AggregateFunctionWindowFunnelData
{
    using TimestampEvent = std::pair<UInt64, UInt32>;

    static constexpr size_t bytes_on_stack = 64;
    using TimestampEvents = PODArray<TimestampEvent, bytes_on_stack,  AllocatorWithStackMemory<Allocator<false>, bytes_on_stack>>;
    
    using Comparator = ComparePairFirst;

    bool sorted = true;
    TimestampEvents events_list;

    void add(UInt64 timestamp, UInt32 event)
    {
        // Since most events should have already been sorted by timestamp.
        if (sorted && events_list.size() > 0 && events_list.back().first > timestamp)
            sorted = false;
        events_list.emplace_back(timestamp, event);
    }

    void merge(const AggregateFunctionWindowFunnelData & other)
    {
        const auto size = events_list.size();

        events_list.insert(std::begin(other.events_list), std::end(other.events_list));

        /// either sort whole container or do so partially merging ranges afterwards
        if (!sorted && !other.sorted)
            std::sort(std::begin(events_list), std::end(events_list), Comparator{});
        else
        {
            const auto begin = std::begin(events_list);
            const auto middle = std::next(begin, size);
            const auto end = std::end(events_list);

            if (!sorted)
                std::sort(begin, middle, Comparator{});

            if (!other.sorted)
                std::sort(middle, end, Comparator{});

            std::inplace_merge(begin, middle, end, Comparator{});
        }

        sorted = true;
    }

    void sort()
    {
        if (!sorted)
       {
            std::sort(std::begin(events_list), std::end(events_list), Comparator{});
            sorted = true;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        writeBinary(events_list.size(), buf);

        for (const auto & events : events_list)
        {
            writeBinary(events.first, buf);
            writeBinary(events.second, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(sorted, buf);

        size_t size;
        readBinary(size, buf);

        events_list.clear();
        events_list.resize(size);

        UInt64 timestamp;
        UInt32 event;

        for (size_t i = 0; i < size; ++i)
        {
            readBinary(timestamp, buf);
            readBinary(event, buf);
            events_list.emplace_back(timestamp, event);
        }
    }

    std::string toString() const{
        std::ostringstream oss;

        for(size_t i = 0; i < events_list.size(); ++i)
        {
            oss << events_list[i].first;
            oss << ":";
            oss << events_list[i].second;
            oss << ",";
        }
        oss << "END";
        return oss.str();
    }
};

class AggregateFunctionWindowFunnel final : public IAggregateFunctionDataHelper<AggregateFunctionWindowFunnelData, AggregateFunctionWindowFunnel>
{
private:
    using Events = UInt32[32];

    UInt64 window;
    Events check_events;
    Logger * log = &Logger::get("AggregateFunctionWindowFunnel");
    size_t check_events_size;

    inline size_t findEventLevel(UInt32 event) const
    {
        for (size_t i = 0; i < check_events_size; i++)
        {
            if (event == check_events[i])
            {
                return i + 1;
            }
        }
        return 0xFFFF;
    }

    UInt32 match(const AggregateFunctionWindowFunnelData & data) const
    {
        if (check_events_size == 1)
            return 1;

        const_cast<AggregateFunctionWindowFunnelData &>(data).sort();

        auto total_len = data.events_list.size();
        size_t max_level = 0;
        for (size_t i = total_len; i > 0; i--)
        {
            auto event = (data.events_list)[i - 1].second;
            auto event_level = findEventLevel(event);
            if (event_level <= max_level)
                continue;

            if (search(data, i, event_level))
            {
                max_level = event_level;
                if (max_level == check_events_size)
                    break;
            }
        }
        //LOG_TRACE(log,  "event_size=>" <<  total_len << "level=>" <<  max_level << " str=>" << data.toString() );
        return max_level;
    }

    inline bool search(const AggregateFunctionWindowFunnelData & data, size_t end_event_pos, size_t end_event_level) const
    {
        if (end_event_level == 1)
        {
            return true;
        }
        // Note the index range
        auto edge_time = (data.events_list)[end_event_pos - 1].first - window;
        auto event_level = end_event_level;
        for (size_t i = end_event_pos; i > 0; i--)
        {
            auto time_event = (data.events_list)[i - 1];
            if (time_event.first < edge_time)
                return false;
            if (check_events[event_level - 1] == time_event.second)
            {
                event_level--;
                if (event_level == 0)
                    return true;
            }
        }
        return false;
    }

public:

    String getName() const override { return "windowFunnel"; }

    AggregateFunctionWindowFunnel(const DataTypes & arguments, const Array & params)
    {
        DataTypePtr timestampType = arguments[0];
        DataTypePtr eventType = arguments[1];

        if (!(timestampType->isUnsignedInteger()))
            throw Exception("Illegal type " + timestampType->getName() + " of argument for aggregate function " + getName() + " (1 arg, timestamp: UIntXX)",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!(eventType->isUnsignedInteger())  || eventType->getName() == "UInt64" )
            throw Exception("Illegal type " + eventType->getName() + " of argument for aggregate function " + getName() + " (2 arg, event id: UIntXX except UInt64)",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (params.size() <= 1 || params.size() > 33)
            throw Exception("Aggregate function " + getName() + " requires (windows_in_seconds, 1_to_32_event_ids).", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        window = params[0].safeGet<UInt64>();
        check_events_size = params.size() - 1;
        for(size_t i = 1; i < params.size(); i ++){
            check_events[i - 1] = static_cast<UInt32>(params[i].safeGet<UInt64>());
        }
       // LOG_TRACE(log, std::fixed << std::setprecision(3) << "setParameters, check_events: " << toString(check_events));
    }



    std::string toString(Events events){
		std::ostringstream oss;

		for(size_t i = 0; i < check_events_size; ++i)
		{	
			oss << events[i];
			oss << ",";
		}
		oss << "END";
		return oss.str();
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void add(AggregateDataPtr place, const IColumn ** column_timestamp, const size_t row_num, Arena *) const override
    {
        this->data(place).add( //
          static_cast<const ColumnVector<UInt64> *>(column_timestamp[0])->getData()[row_num],
          static_cast<const ColumnVector<UInt32> *>(column_timestamp[1])->getData()[row_num]
        );
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnUInt8 &>(to).getData().push_back(match(this->data(place)));
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

}
