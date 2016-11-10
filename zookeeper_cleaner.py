__author__ = 'nbaruah'

import logging
import os
import sys
from optparse import OptionParser

bin_dir = os.path.dirname(os.path.realpath(__file__))
lib_path = os.path.join(bin_dir, 'setup', 'scripts')
sys.path.append(lib_path)

import constants
import util_functions
from kazoo.client import KazooClient

# -e environment [Required]
# -p parent-node (exdr/reference/process) [Required]
# -n comma separeted list of child node names
# -a reset all interfaces 
# -c complement the behaviuor
# -n and -a are mutually exclusive
# Use -c with -n option only

OPT_ENV = "-e"
OPT_PARENT_NODE = "-p" 
OPT_NODES = "-n"
OPT_ALL_RESET = "-a"
OPT_COMPLEMENT = "-c"
OPT_LOG_LEVEL = "--log"
PNODE_LIST = ['exdr', 'reference', 'process']

def main():
    parser_obj = setup_parser()
    (options, args) = parser_obj.parse_args()

    try:
        configure_logging(options)
        validate_inputs(options)
    except Exception as e:
        logging.error(e)
        sys.exit(-1)

    env = options.env
    build_dir = os.path.dirname(bin_dir)
    config_dir = os.path.join(build_dir, constants.CONFIG_DIR)
    properties = util_functions.load_properties(config_dir, env)
    zk_conn_string = properties.get(constants.ZK_CON_STRING)
    zk_base_path = properties.get(constants.ZK_LAYOUT_BASE) + "/" + env + "/" + options.p_node

    try:
        if options.all_reset:
            logging.info('Resetting all nodes under %s', zk_base_path)
            child_nodes = get_child_nodes(zk_base_path, zk_conn_string)
            reset_znodes(zk_base_path, child_nodes, zk_conn_string)
        elif options.nodes and not options.complement:
            logging.info('Resetting node(s) [%s]', options.nodes)
            child_nodes = set(options.nodes.split(","))
            reset_znodes(zk_base_path, child_nodes, zk_conn_string)
        elif options.nodes and options.complement:
            all_child_nodes = set(get_child_nodes(zk_base_path, zk_conn_string))
            excluded_child_nodes = set(options.nodes.split(","))
            child_nodes = all_child_nodes - excluded_child_nodes
            logging.info('Resetting node(s) [%s]', ','.join(child_nodes))
            reset_znodes(zk_base_path, child_nodes, zk_conn_string)
    except Exception as e:
        logging.error(e)

def setup_parser():
    parser_obj = OptionParser()
    parser_obj.add_option(OPT_ENV, dest="env", help="Environment name e.g. dev, prd, tst")
    parser_obj.add_option(OPT_PARENT_NODE, dest="p_node", help="Name of the zookeeper parent node e.g. exdr, reference, process")
    parser_obj.add_option(OPT_NODES, dest="nodes", help="comma seperated list of node names under parent node")
    parser_obj.add_option(OPT_ALL_RESET, action="store_true", dest="all_reset", default=False, help="Reset all nodes under parent node")
    parser_obj.add_option(OPT_COMPLEMENT, action="store_true", dest="complement", default=False , help="Complement the actions")
    parser_obj.add_option(OPT_LOG_LEVEL, dest="log_level", default='INFO', help="Set the logging level")
    return parser_obj

def configure_logging(options):
    log_level = options.log_level
    numeric_level = getattr(logging, log_level.upper())
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log_level)
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=numeric_level)

def validate_inputs(options):
    if options.env is None:
        raise Exception(" -e (Environment) is required argument")
    if options.p_node is None:
        raise Exception(" -p (Parent-node name) is required argument")
    if options.p_node not in PNODE_LIST:
        raise Exception(" Invalid Parent-node. Possible parent nodes are: [" + ', '.join(PNODE_LIST) + ']')
    if not options.nodes and not options.all_reset:
        raise Exception(" Provide either -a or -n ")
    if options.nodes and options.all_reset:
        raise Exception(" Options -n and -a are mutually exclusive")

def get_child_nodes(zk_base_path, zk_conn_string):
    try:
        zk = KazooClient(hosts=zk_conn_string)
        zk.start()
        if zk.exists(zk_base_path):
            logging.info('Getting child nodes for %s', zk_base_path)
            return zk.get_children(zk_base_path)
    except Exception:
        logging.error('Error occured while getting child nodes for %s', zk_base_path)
        raise
    finally:
        zk.stop()

def reset_znodes(zk_base_path, child_nodes_set, zk_conn_string):
    try:
        zk = KazooClient(hosts=zk_conn_string)
        zk.start()
        for child in child_nodes_set:
            path = zk_base_path + "/" + child.strip()
            if zk.exists(path):
                zk.set(path, b"%s" % constants.ZK_DEFAULT_NODE_VALUE)
                logging.info('Reset successful for zk-node %s', path)
    except Exception:
        logging.error('Error occured while resetting nodes')
        raise
    finally:
        zk.stop()

if __name__ == '__main__':
    main()
