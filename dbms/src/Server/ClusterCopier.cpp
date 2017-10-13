#include "ClusterCopier.h"
#include <boost/program_options.hpp>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/getFQDNOrHostName.h>
#include <Client/Connection.h>
#include <Interpreters/Settings.h>
#include <Interpreters/Cluster.h>
#include <Poco/Util/Application.h>
#include <Poco/UUIDGenerator.h>

#include <common/logger_useful.h>
#include <common/ApplicationServerExt.h>
#include <Interpreters/InterpreterCheckQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/DescribeStreamFactory.h>
#include <Common/ClickHouseRevision.h>
#include <Common/escapeForFileName.h>
#include <Storages/StorageDistributed.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Databases/DatabaseMemory.h>
#include <Poco/File.h>


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

struct TableTask
{
    TableTask(const Poco::Util::AbstractConfiguration & config, const String & prefix, const String & table_key);

    DatabaseAndTableName table_pull;
    DatabaseAndTableName table_push;
    String db_table_pull;
    String db_table_push;

    String engine_str;
    String sharding_key_str;
    String where_condition_str;
    String tag_in_config;

    String table_structure_pull;
    NamesAndTypesListPtr names_and_types_pull;

    String table_create_str_push;
};


struct ClusterTask
{
    ClusterTask(const Poco::Util::AbstractConfiguration & config, const String & base_key);

    size_t max_workers = 0;
    Settings settings_pull;
    Settings settings_push;

    String clusters_prefix;
    String cluster_pull_name;
    String cluster_push_name;

    std::list<TableTask> table_tasks;
};


TableTask::TableTask(const Poco::Util::AbstractConfiguration & config, const String & prefix, const String & table_key)
{
    String table_prefix = prefix + "." + table_key + ".";

    tag_in_config = table_key;

    table_pull.first = config.getString(table_prefix + "database_pull");
    table_pull.second = config.getString(table_prefix + "table_pull");
    db_table_pull = backQuoteIfNeed(table_pull.first) + backQuoteIfNeed(table_pull.second);

    table_push.first = config.getString(table_prefix + "database_push");
    table_push.second = config.getString(table_prefix + "table_push");
    db_table_push = backQuoteIfNeed(table_push.first) + backQuoteIfNeed(table_push.second);

    engine_str = config.getString(table_prefix + "engine", "");
    sharding_key_str = config.getString(table_prefix + "sharding_key", "");
    where_condition_str = config.getString(table_prefix + "where_condition", "");
}


ClusterTask::ClusterTask(const Poco::Util::AbstractConfiguration & config, const String & base_key)
{
    String prefix = base_key.empty() ? "" : base_key + ".";

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

    cluster_pull_name = config.getString(prefix + "cluster_pull");
    cluster_push_name = config.getString(prefix + "cluster_push");

    Poco::Util::AbstractConfiguration::Keys tables_keys;
    config.keys(prefix + "tables", tables_keys);

    for (const auto & table_key : tables_keys)
    {
        table_tasks.emplace_back(config, prefix + "tables", table_key);
    }
}

}


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
        auto zookeeper = getZooKeeper();

        String description_path = task_zookeeper_path + "/description";
        String task_config_str = zookeeper->get(description_path);

        task_cluster_config = getConfigurationFromXMLString(task_config_str);
        task_cluster = std::make_unique<ClusterTask>(*task_cluster_config, "");

        context.setClustersConfig(task_cluster_config, task_cluster->clusters_prefix);
        cluster_pull = context.getCluster(task_cluster->cluster_pull_name);
        cluster_push = context.getCluster(task_cluster->cluster_push_name);
    }

    void process()
    {
        if (!task_cluster)
            throw Exception("Cluster task is not initialized", ErrorCodes::LOGICAL_ERROR);

        for (auto & task_table : task_cluster->table_tasks)
        {
            String columns_structure = getTableStructureAndCheckConsistency(task_table);
            task_table.table_structure_pull = columns_structure;

            ParserColumnDeclarationList columns_p;
            const char * begin = columns_structure.data();
            ASTPtr columns_pull = parseQuery(columns_p, begin, begin + columns_structure.size(), "column declaration list");

            //auto storage_pull = StorageDistributed::createWithOwnCluster("remote", );
        }
    }

protected:

    String getTableStructureAndCheckConsistency(TableTask & table_task)
    {
        InterpreterCheckQuery::RemoteTablesInfo remotes_info;
        remotes_info.cluster = cluster_pull;
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
                                        task_cluster->cluster_pull_name, ErrorCodes::BAD_ARGUMENTS);

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
    std::unique_ptr<ClusterTask> task_cluster;

    ClusterPtr cluster_pull;
    ClusterPtr cluster_push;

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
