#include <db.hpp>

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/config.hpp>
#include <boost/program_options/environment_iterator.hpp>
#include <boost/program_options/eof_iterator.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/version.hpp>

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
    std::string log_level = "";
    unsigned thread_count = 0;
    std::string path = "";
    po::options_description desc("Options:");
    desc.add_options()("help", " receiving information\n")(
        "log_level", po::value<std::string>(&log_level), "= info|warning|error\n = default: error\n")(

            "output", po::value<std::string>(&path),
            "=  \n = default: count of logical core\n Output directory MUST BE EMPTY\n")(

                "thread_count", po::value<unsigned>(&thread_count),
                "= <path/to/output_directory\n= default: /home/vagrant/Projects/lab-10-kv-storage/hash\n");

    po::variables_map vm;

    po::store(po::parse_command_line(argc, argv, desc), vm);

    po::notify(vm);
    if (vm.count("help")) {

        std::cout << "Usage: dbcs [options] <path/to/input/storage.db>" << std::endl;

        std::cout << desc << std::endl;
    }
    kv_storage kv;
    kv.set_log_level(log_level);
    kv.set_thread_count(thread_count);
    kv.set_path_write(path);
    kv.make_process();
}

