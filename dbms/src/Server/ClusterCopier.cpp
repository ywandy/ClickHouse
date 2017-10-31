#include "ClusterCopier.h"
#include <boost/program_options.hpp>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Util/Application.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/File.h>
#include <chrono>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/getFQDNOrHostName.h>
#include <Client/Connection.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterpreterCheckQuery.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/DescribeStreamFactory.h>

#include <common/logger_useful.h>
#include <common/ApplicationServerExt.h>
#include <Parsers/ASTCheckQuery.h>
#include <Common/typeid_cast.h>
#include <Common/ClickHouseRevision.h>
#include <Common/escapeForFileName.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/StorageDistributed.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Databases/DatabaseMemory.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <IO/Operators.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Common/isLocalAddress.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int BAD_ARGUMENTS;
}


using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;


template <typename T>
static ConfigurationPtr getConfigurationFromXMLString(T && xml_string)
{
    std::stringstream ss;
    ss << std::forward<T>(xml_string);

    Poco::XML::InputSource input_source(ss);
    ConfigurationPtr res(new Poco::Util::XMLConfiguration(&input_source));

    return res;
}

namespace
{

using DatabaseAndTableName = std::pair<String, String>;


struct TaskCluster;
struct TaskTable;
struct TaskShard;
struct TaskPartition;

struct TaskPartition
{
    TaskPartition(TaskShard & parent, const String & name_) : task_shard(parent), name(name_) {}

    String getZooKeeperPath() const;

    TaskShard & task_shard;
    String name;

    String create_query_pull;
    ASTPtr create_query_pull_ast;
    ASTPtr create_query_push;
};

using TasksPartition = std::vector<TaskPartition>;


using ShardInfo = Cluster::ShardInfo;

struct TaskShard
{
    TaskShard(TaskTable & parent, const ShardInfo & info_) : task_table(parent), info(info_) {}

    String getZooKeeperPath() const;

    TaskTable & task_table;

    ShardInfo info;
    ConnectionPoolWithFailover::Entry connection_entry;

    TasksPartition partitions;
};

using TaskShardPtr = TaskShard *;
using TasksShard = std::vector<TaskShard>;
using TasksShardPtrs = std::vector<TaskShard *>;


struct TaskTable
{
    TaskTable(TaskCluster & parent, const Poco::Util::AbstractConfiguration & config, const String & prefix,
                  const String & table_key);

    String getZooKeeperPath() const;

    TaskCluster & task_cluster;
    String name_in_config;

    String cluster_pull_name;
    String cluster_push_name;

    DatabaseAndTableName table_pull;
    DatabaseAndTableName table_push;
    String db_table_pull;
    String db_table_push;

    String engine_str;
    ASTPtr engine_ast;

    String sharding_key_str;
    ASTPtr sharding_key_ast;

    String where_condition_str;
    ASTPtr where_condition_ast;

    struct Shards
    {
        TasksShard all_shards;
        TasksShardPtrs all_shards_prioritized;

        TasksShardPtrs local_shards;
        TasksShardPtrs remote_shards;
    };

    ClusterPtr cluster_pull;
    Shards shards_pull;

    template <class URNG>
    void initShards(URNG && urng);

    ClusterPtr cluster_push;
};

using TasksTable = std::list<TaskTable>;


struct TaskCluster
{
    TaskCluster(const String & task_zookeeper_path_, const Poco::Util::AbstractConfiguration & config, const String & base_key);

    String task_zookeeper_path;

    String getZooKeeperPath() const
    {
        return task_zookeeper_path;
    }

    size_t max_workers = 0;
    Settings settings_pull;
    Settings settings_push;

    String clusters_prefix;

    TasksTable table_tasks;

    std::random_device rd;
    std::mt19937 random_generator;
};


String getDatabaseDotTableQuoted(const String & database, const String & table)
{
    return backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}

String getDatabaseDotTableQuoted(const DatabaseAndTableName & db_and_table)
{
    return getDatabaseDotTableQuoted(db_and_table.first, db_and_table.second);
}


String TaskPartition::getZooKeeperPath() const
{
    return task_shard.getZooKeeperPath() + "/" + name;
}

String TaskShard::getZooKeeperPath() const
{
    return task_table.getZooKeeperPath() + "/" + toString(info.shard_num);
}

String TaskTable::getZooKeeperPath() const
{
    return task_cluster.getZooKeeperPath()
           + "/" + cluster_pull_name
           + "/" + backQuoteIfNeed(table_pull.first)
           + "/" + backQuoteIfNeed(table_pull.second);
}


TaskTable::TaskTable(TaskCluster & parent, const Poco::Util::AbstractConfiguration & config, const String & prefix_,
                     const String & table_key)
: task_cluster(parent)
{
    String table_prefix = prefix_ + "." + table_key + ".";

    name_in_config = table_key;

    cluster_pull_name = config.getString(table_prefix + "cluster_pull");
    cluster_push_name = config.getString(table_prefix + "cluster_push");

    table_pull.first = config.getString(table_prefix + "database_pull");
    table_pull.second = config.getString(table_prefix + "table_pull");
    db_table_pull = backQuoteIfNeed(table_pull.first) + backQuoteIfNeed(table_pull.second);

    table_push.first = config.getString(table_prefix + "database_push");
    table_push.second = config.getString(table_prefix + "table_push");
    db_table_push = backQuoteIfNeed(table_push.first) + backQuoteIfNeed(table_push.second);

    engine_str = config.getString(table_prefix + "engine");
    {
        ParserStorage parser_storage;
        engine_ast = parseQuery(parser_storage, engine_str, "engine");
    }

    sharding_key_str = config.getString(table_prefix + "sharding_key");
    if (!sharding_key_str.empty())
    {
        ParserExpressionList parser_expression_list(false);
        sharding_key_ast = parseQuery(parser_expression_list, sharding_key_str, "sharding_key");
    }

    where_condition_str = config.getString(table_prefix + "where_condition", "");
    if (!where_condition_str.empty())
    {
        ParserExpressionWithOptionalAlias parser_expression(false);
        where_condition_ast = parseQuery(parser_expression, where_condition_str, "where_condition");
    }
}


template<class URNG>
void TaskTable::initShards(URNG && urng)
{
    for (auto & shard_info : cluster_pull->getShardsInfo())
        shards_pull.all_shards.emplace_back(*this, shard_info);

    auto has_local_addresses = [] (const ShardInfo & info, const ClusterPtr & cluster) -> bool
    {
        if (info.isLocal()) /// isLocal() checks ports that aren't match
            return true;

        for (auto & address : cluster->getShardsAddresses().at(info.shard_num))
        {
            if (isLocalAddress(address.resolved_address))
                return true;
        }

        return false;
    };

    for (TaskShard & shard : shards_pull.all_shards)
    {
        if (has_local_addresses(shard.info, cluster_pull))
            shards_pull.local_shards.push_back(&shard);
        else
            shards_pull.remote_shards.push_back(&shard);
    }

    // maybe it is not necessary to shuffle local addresses
    std::shuffle(shards_pull.local_shards.begin(), shards_pull.local_shards.end(), std::forward<URNG>(urng));
    std::shuffle(shards_pull.remote_shards.begin(), shards_pull.remote_shards.end(), std::forward<URNG>(urng));

    shards_pull.all_shards_prioritized.insert(shards_pull.all_shards_prioritized.end(),
                                              shards_pull.local_shards.begin(), shards_pull.local_shards.end());
    shards_pull.all_shards_prioritized.insert(shards_pull.all_shards_prioritized.end(),
                                              shards_pull.remote_shards.begin(), shards_pull.remote_shards.end());
}


TaskCluster::TaskCluster(const String & task_zookeeper_path_, const Poco::Util::AbstractConfiguration & config, const String & base_key)
{
    String prefix = base_key.empty() ? "" : base_key + ".";

    task_zookeeper_path = task_zookeeper_path_ + "/tasks";

    max_workers = config.getUInt64(prefix + "max_workers");

    if (config.has(prefix + "settings"))
    {
        settings_pull.loadSettingsFromConfig(prefix + "settings", config);
        settings_push.loadSettingsFromConfig(prefix + "settings", config);
    }

    if (config.has(prefix + "settings_pull"))
        settings_pull.loadSettingsFromConfig(prefix + "settings_pull", config);

    if (config.has(prefix + "settings_push"))
        settings_push.loadSettingsFromConfig(prefix + "settings_push", config);

    clusters_prefix = prefix + "remote_servers";

    if (!config.has(clusters_prefix))
        throw Exception("You should specify list of clusters in " + clusters_prefix, ErrorCodes::BAD_ARGUMENTS);

    Poco::Util::AbstractConfiguration::Keys tables_keys;
    config.keys(prefix + "tables", tables_keys);

    for (const auto & table_key : tables_keys)
    {
        std::cerr << "Loading table " << table_key << "\n";
        table_tasks.emplace_back(*this, config, prefix + "tables", table_key);
        std::cerr << "Loading table " << table_key << "\n";
    }
}

} // end of an anonymous namespace


class ClusterCopier
{
public:

    ClusterCopier(const ConfigurationPtr & zookeeper_config_,
                  const String & task_path_,
                  const String & host_id_,
                  const String & proxy_database_name_,
                  Context & context_,
                  Poco::Logger * log_)
    :
        zookeeper_config(zookeeper_config_),
        task_zookeeper_path(task_path_),
        host_id(host_id_),
        proxy_database_name(proxy_database_name_),
        context(context_),
        log(log_)
    {
        initZooKeeper();
    }

    void init()
    {
        String description_path = task_zookeeper_path + "/description";
        String task_config_str = getZooKeeper()->get(description_path);

        task_cluster_config = getConfigurationFromXMLString(task_config_str);
        task_cluster = std::make_unique<TaskCluster>(task_zookeeper_path, *task_cluster_config, "");

        /// Override critical settings
        Settings & settings_pull = task_cluster->settings_pull;
        /// To prefer local replicas instead of remote ones
        settings_pull.load_balancing = LoadBalancing::NEAREST_HOSTNAME;
        settings_pull.limits.readonly = 1;

        /// Set up clusters
        context.setClustersConfig(task_cluster_config, task_cluster->clusters_prefix);

        /// Set up shards and their priority
        task_cluster->random_generator.seed(task_cluster->rd());
        for (auto & task_table : task_cluster->table_tasks)
        {
            std::cerr << "Initializing shards " << task_table.db_table_pull << "\n";
            task_table.cluster_pull = context.getCluster(task_table.cluster_pull_name);
            task_table.cluster_push = context.getCluster(task_table.cluster_push_name);
            task_table.initShards(task_cluster->random_generator);
            std::cerr << "Initialized shards " << task_table.db_table_pull << "\n";
        }

        /// Compute set of partitions, set of partitions aren't changed
        for (auto & task_table : task_cluster->table_tasks)
        {
            for (TaskShardPtr task_shard : task_table.shards_pull.all_shards_prioritized)
            {
                if (task_shard->info.pool == nullptr)
                {
                    throw Exception("It is impossible to have only local shards, at least port number must be different",
                                    ErrorCodes::LOGICAL_ERROR);
                }

                LOG_DEBUG(log, "Set up table task " << task_table.name_in_config << " ("
                               << "cluster " << task_table.cluster_pull_name
                               << ", table " << task_table.db_table_pull
                               << ", shard " << task_shard->info.shard_num << ")");

                LOG_DEBUG(log, "There are "
                    << task_table.shards_pull.local_shards.size() << " local shards, and "
                    << task_table.shards_pull.remote_shards.size() << " remote ones");

                task_shard->connection_entry = task_shard->info.pool->get(&task_cluster->settings_pull);
                LOG_DEBUG(log, "Will use " << task_shard->connection_entry->getDescription());

                Strings partitions = getPartitions(task_table.table_pull, *task_shard->connection_entry, task_cluster->settings_pull);
                for (const String & partition_name : partitions)
                    task_shard->partitions.emplace_back(*task_shard, partition_name);

                LOG_DEBUG(log, "Will fetch " << task_shard->partitions.size() << " parts");
            }
        }

        auto zookeeper = getZooKeeper();
        zookeeper->createAncestors(getWorkersPath() + "/");
    }

    void process()
    {
        for (TaskTable & task_table : task_cluster->table_tasks)
        {
            for (TaskShardPtr task_shard : task_table.shards_pull.all_shards_prioritized)
            {
                for (TaskPartition & task_partition : task_shard->partitions)
                {
                    processPartitionTask(task_partition);
                }
            }
        }
    }

protected:

    String getWorkersPath() const
    {
        return task_cluster->getZooKeeperPath() + "/workers";
    }

    String getCurrentWorkerNodePath() const
    {
        return getWorkersPath() + "/" + host_id;
    }

    zkutil::EphemeralNodeHolder::Ptr createWorkerNodeAndWaitIfNeed(const zkutil::ZooKeeperPtr & zookeeper, const String & task_description)
    {
        while (true)
        {
            auto zookeeper = getZooKeeper();

            zkutil::Stat stat;
            zookeeper->get(getWorkersPath(), &stat);

            if (stat.numChildren >= task_cluster->max_workers)
            {
                LOG_DEBUG(log, "Too many workers (" << stat.numChildren << ", maximum " << task_cluster->max_workers << ")"
                    << ". Postpone processing " << task_description);

                using namespace std::literals::chrono_literals;
                std::this_thread::sleep_for(10s);
            }
            else
            {
                return std::make_shared<zkutil::EphemeralNodeHolder>(getCurrentWorkerNodePath(), *zookeeper, true, false, task_description);
            }
        }
    }

    ASTPtr rewriteCreateQueryForPush(const ASTPtr & create_query_pull, TaskTable & task_table)
    {
        auto & create = typeid_cast<ASTCreateQuery &>(*create_query_pull);
        auto res = std::make_shared<ASTCreateQuery>(create);

        if (create.storage == nullptr || task_table.engine_ast == nullptr)
            throw Exception("Storage is not specified", ErrorCodes::LOGICAL_ERROR);

        res->children.clear();
        res->set(res->columns, create.columns->clone());
        res->set(res->storage, task_table.engine_ast->clone());

        return res;
    }

    ASTPtr rewriteCreateQueryForProxy(const ASTPtr & create_query_pull, TaskTable & task_table)
    {
        auto & create = typeid_cast<ASTCreateQuery &>(*create_query_pull);
        auto res = std::make_shared<ASTCreateQuery>(create);

        if (create.storage == nullptr)
            throw Exception("Storage is not specified", ErrorCodes::LOGICAL_ERROR);

        if (task_table.sharding_key_ast == nullptr)
            throw Exception("Sharding key is not specified", ErrorCodes::LOGICAL_ERROR);

        ASTPtr storage_ast;
        String storage_str = "ENGINE = Distributed" + queryToString(task_table.sharding_key_ast);
        std::cerr << "storage_str " << storage_str << "\n";
        {
            ParserStorage parser_storage;
            storage_ast = parseQuery(parser_storage, storage_str, "storage");
        }

        res->children.clear();
        res->set(res->columns, create.columns->clone());
        res->set(res->storage, storage_ast);

//        auto storage_ast = std::make_shared<ASTStorage>();
//        auto engine = std::make_shared<ASTFunction>();
//        engine->name = "Distributed";
//        engine->arguments =
//        storage_ast->set(storage_ast->engine, );

        return res;
    }

    void processPartitionTask(TaskPartition & task_partition)
    {
        auto zookeeper = getZooKeeper();

        String partition_task_node = task_partition.getZooKeeperPath();
        String partition_task_active_node = partition_task_node + "/active_worker";
        String partition_task_status_node = partition_task_node + "/state";
        zookeeper->createAncestors(partition_task_active_node);

        auto worker_node_holder = createWorkerNodeAndWaitIfNeed(zookeeper, partition_task_node);
        auto partition_task_node_holder = std::make_shared<zkutil::EphemeralNodeHolder>(
            partition_task_active_node, *zookeeper, true, false, host_id);

        TaskShard & task_shard = task_partition.task_shard;
        TaskTable & task_table = task_shard.task_table;

        /// We need to update table definitions for each part, it could be changed after ALTER

        String create_query_pull_str = getCreateTable(task_table.table_pull, *task_shard.connection_entry,
                                                  task_cluster->settings_pull);

        ParserCreateQuery parser_create_query;
        ASTPtr create_query_pull_ast = parseQuery(parser_create_query, create_query_pull_str, "CREATE TABLE");

        ASTPtr create_query_push_ast = rewriteCreateQueryForPush(create_query_pull_ast, task_table);
        ASTPtr create_query_proxy_ast = rewriteCreateQueryForProxy(create_query_pull_ast, task_table);

        LOG_DEBUG(log, "Push create query: " << queryToString(create_query_push_ast));
        LOG_DEBUG(log, "Distributed table create query: " << queryToString(create_query_proxy_ast));

        zookeeper->tryCreate(partition_task_status_node, "0", zkutil::CreateMode::Persistent);
    }

    static BlockInputStreamPtr squashStreamIntoOneBlock(const BlockInputStreamPtr & stream)
    {
        return std::make_shared<SquashingBlockInputStream>(
            stream,
            std::numeric_limits<size_t>::max(),
            std::numeric_limits<size_t>::max()
        );
    }

    Strings getPartitions(const DatabaseAndTableName & table, Connection & connection, const Settings & settings)
    {
        std::stringstream ss;
        ss << "SELECT DISTINCT partition FROM system.parts WHERE"
           << " database = " << DB::quote << table.first
           << " AND table = " << DB::quote << table.second;

        auto input = squashStreamIntoOneBlock(std::make_shared<RemoteBlockInputStream>(connection, ss.str(), context, &settings));
        Block block = input->read();

        Strings res;
        if (block)
        {
            ColumnString & partition_col = typeid_cast<ColumnString &>(*block.getByName("partition").column);
            for (size_t i = 0; i < partition_col.size(); ++i)
                res.push_back(partition_col.getDataAt(i).toString());
        }

        return res;
    }

    String getCreateTable(const DatabaseAndTableName & table, Connection & connection, const Settings & settings)
    {
        std::stringstream ss;
        ss << "SHOW CREATE TABLE " << getDatabaseDotTableQuoted(table);

        auto input = squashStreamIntoOneBlock(std::make_shared<RemoteBlockInputStream>(connection, ss.str(), context, &settings));
        Block block = input->read();

        ColumnString & create_col = typeid_cast<ColumnString &>(*block.safeGetByPosition(0).column);
        return create_col.getDataAt(0).toString();
    }

    String getTableStructureAndCheckConsistency(TaskTable & table_task)
    {
        InterpreterCheckQuery::RemoteTablesInfo remotes_info;
        remotes_info.cluster = table_task.cluster_pull;
        remotes_info.remote_database = table_task.table_pull.first;
        remotes_info.remote_table = table_task.table_pull.second;

        Context local_context = context;
        InterpreterCheckQuery check_table(std::move(remotes_info), local_context);

        BlockIO io = check_table.execute();
        if (io.out != nullptr)
            throw Exception("Expected empty io.out", ErrorCodes::LOGICAL_ERROR);

        String columns_structure;
        size_t rows = 0;

        Block block;
        while ((block = io.in->read()))
        {
            auto & structure_col = typeid_cast<ColumnString &>(*block.getByName("structure").column);
            auto & structure_class_col = typeid_cast<ColumnUInt32 &>(*block.getByName("structure_class").column);

            for (size_t i = 0; i < block.rows(); ++i)
            {
                if (structure_class_col.getElement(i) != 0)
                    throw Exception("Structures of table " + table_task.db_table_pull + " are different on cluster " +
                                        table_task.cluster_pull_name, ErrorCodes::BAD_ARGUMENTS);

                if (rows == 0)
                    columns_structure = structure_col.getDataAt(i).toString();

                ++rows;
            }
        }

        return columns_structure;
    }

    void initZooKeeper()
    {
        current_zookeeper = std::make_shared<zkutil::ZooKeeper>(*zookeeper_config, "zookeeper");
    }

    const zkutil::ZooKeeperPtr & getZooKeeper()
    {
        if (!current_zookeeper)
            throw Exception("Cannot get ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

        return current_zookeeper;
    }

private:
    ConfigurationPtr zookeeper_config;
    String task_zookeeper_path;
    String host_id;
    String proxy_database_name;

    ConfigurationPtr task_cluster_config;
    std::unique_ptr<TaskCluster> task_cluster;

    zkutil::ZooKeeperPtr current_zookeeper;

    Context & context;
    Poco::Logger * log;
};


class ClusterCopierApp : public Poco::Util::Application
{
public:

    void initialize(Poco::Util::Application & self) override
    {
        Poco::Util::Application::initialize(self);
    }

    void init(int argc, char ** argv)
    {
        /// Poco's program options are quite buggy, use boost's one
        namespace po = boost::program_options;

        po::options_description options_desc("Allowed options");
        options_desc.add_options()
            ("help", "produce this help message")
            ("config-file,c", po::value<std::string>(&config_xml)->required(), "path to config file with ZooKeeper config")
            ("task-path,p", po::value<std::string>(&task_path)->required(), "path to task in ZooKeeper")
            ("log-level", po::value<std::string>(&log_level)->default_value("error"), "log level");

        po::positional_options_description positional_desc;
        positional_desc.add("config-file", 1);
        positional_desc.add("task-path", 1);

        po::variables_map options;
        po::store(po::command_line_parser(argc, argv).options(options_desc).positional(positional_desc).run(), options);

        if (options.count("help"))
        {
            std::cerr << "Copies tables from one cluster to another" << std::endl;
            std::cerr << "Usage: clickhouse copier <config-file> <task-path>" << std::endl;
            std::cerr << options_desc << std::endl;
        }

        po::notify(options);

        if (config_xml.empty() || !Poco::File(config_xml).exists())
            throw Exception("ZooKeeper configuration file " + config_xml + " doesn't exist", ErrorCodes::BAD_ARGUMENTS);

        //setupLogging(log_level);
    }

    int main(const std::vector<std::string> & args) override
    {
        try
        {
            mainImpl();
        }
        catch (...)
        {
            std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
            auto code = getCurrentExceptionCode();

            return (code ? code : -1);
        }

        return 0;
    }

    void mainImpl()
    {
        ConfigurationPtr zookeeper_configuration(new Poco::Util::XMLConfiguration(config_xml));
        auto log = &logger();

        /// Hostname + random id (to be able to run multiple copiers on the same host)
        process_id = Poco::UUIDGenerator().create().toString();
        host_id = escapeForFileName(getFQDNOrHostName()) + '#' + process_id;
        String clickhouse_path = Poco::Path::current() + "/" + process_id;

        LOG_INFO(log, "Starting clickhouse-copier ("
            << "id " << process_id << ", "
            << "path " << clickhouse_path << ", "
            << "revision " << ClickHouseRevision::get() << ")");

        auto context = std::make_unique<Context>(Context::createGlobal());
        SCOPE_EXIT(context->shutdown());

        context->setGlobalContext(*context);
        context->setApplicationType(Context::ApplicationType::LOCAL);
        context->setPath(clickhouse_path);

        const std::string default_database = "_local";
        context->addDatabase(default_database, std::make_shared<DatabaseMemory>(default_database));
        context->setCurrentDatabase(default_database);

        std::unique_ptr<ClusterCopier> copier(new ClusterCopier(
            zookeeper_configuration, task_path, host_id, default_database, *context, log));

        copier->init();
        copier->process();
    }


private:

    static void setupLogging(const std::string & log_level)
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel);
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
        formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Poco::Logger::root().setChannel(formatting_channel);
        Poco::Logger::root().setLevel(log_level);
    }

    std::string config_xml;
    std::string task_path;
    std::string log_level = "error";

    std::string process_id;
    std::string host_id;
};

}


int mainEntryClickHouseClusterCopier(int argc, char ** argv)
{
    try
    {
        DB::ClusterCopierApp app;
        app.init(argc, argv);
        return app.run();
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();

        return (code ? code : -1);
    }
}
