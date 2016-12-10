# -*- coding: utf8 -*-

__author__ = 'dmitry.arefyev@megafon.ru'

""" 
    Скрипт сохранения конфигурации rabbitmq в svn
"""

import config, os, logging, sys
from requests import get
import json, pysvn


def get_config():
    cfg = config.Config(file(os.getcwd() +'/' + 'saver.cfg'))
    return cfg

def get_login(realm, username, may_save):
    return True, cfg.rep_user, cfg.rep_pass, True

def clone_repository(cfg):
    logging.info('Clone repository %s at %s' % (cfg.rep_name, cfg.rep_dir))
    client = pysvn.Client()
    client.callback_get_login = get_login
    client.checkout('svn://%s:%s@%s/%s' % (cfg.rep_user, cfg.rep_pass, cfg.rep_host, cfg.rep_name), cfg.rep_dir)
    logging.info('Repository %s cloned' % (cfg.rep_name))
    del client

def update_rep(rep_dir):
    client = pysvn.Client()
    logging.info('Update repository %s ' % (rep_dir))
    #client.update(cfg.rep_dir)
    logging.info(client.update(rep_dir))
    return client

def push_rep(client, conf_file, json_name):
    logging.info('Push changes %s ' % (conf_file))
    try:
        a = client.checkin([conf_file], 'Update config %s' % json_name)
        if a == None:
            logging.info('Nothing to change')
        else:
            logging.info('Status: %s' % a)
    except Exception as e:
        if ('is not under version control' in e.message):
            logging.warning('%s is not under version control' % json_name)
            client.add(conf_file)
            a = client.checkin([conf_file], 'Init config file %s' % json_name)
            logging.info('Status: %s' % a)
        else:
            logging.error('%s' % e.message)

def save_json(rep_dir, data, rep_conf, json_name):
    client = update_rep(rep_dir)
    name = os.path.join(rep_conf, json_name)
    with open(name, 'w') as fp:
        json.dump(data, fp)
    push_rep(client, name, json_name)

def get_rabbit_configuration(cfg, N):
    print u'Save RabbitMQ config'
    url = (cfg.rabbit_url % (cfg.rabbit_user, cfg.rabbit_pass)) + cfg.api_config
    logging.info('Get RabbitMQ config from %s' % (cfg.rabbit_url))
    if N != 0:
        R = get(url)
        logging.info('Status code: %s' % R.status_code)
        if R.status_code == 200:
            save_json(cfg.rep_dir, R.content, cfg.rep_rab_conf, cfg.rabbit_name)
        else:
            logging.error('Can\'t get rabbit configuration. Reget.')
            get_rabbit_configuration(cfg, N-1)
    else:
        logging.error('Can\'t get rabbit configuration.')
        return {}

def get_zoo_conf(cfg, host, url, N):
    if N != 0:
        logging.info('Get ZooKeeper config from %s' % (host))
        R = get(url)
        logging.info('Status code: %s' % R.status_code)
        if R.status_code == 200:
            save_json(cfg.rep_dir, R.content, cfg.rep_zoo_conf, (cfg.zoo_name % str(host)))
        else:
            logging.error('Can\'t get ZooKeeper configuration. Reget.')
            get_zoo_conf(cfg, host, url, N-1)
    else:
        logging.error('Can\'t get ZooKeeper configuration.')
        return {}


def get_zoo_configuration(cfg, N):
    print u'Save ZOO config'
    url = cfg.zman_host + cfg.zman_api_conf
    hosts = cfg.zoo_host
    for host in hosts:
        get_zoo_conf(cfg, host, (url % host), N)

def configs_save(cfg):
    get_rabbit_configuration(cfg, 5)
    get_zoo_configuration(cfg, 5)

if __name__ == "__main__":
    cfg = get_config()
    logging.basicConfig(filename=os.getcwd() +'/logs/' + cfg.log_file, level=cfg.log_level, format='%(asctime)-15s %(levelname)s %(message)s')
    logging.info('start %s' % cfg.app_name)
    if (len(sys.argv) > 1):
        if sys.argv[1] == 'start':
            print u'Clone repository'
            try:
                clone_repository(cfg)
            except Exception as e:
                print u'Something wrong. Please ckeck the log'
                logging.info('Error: %s' % (e.message))
        elif sys.argv[1] == 'config':
            if(len(sys.argv) == 2):
                configs_save(cfg)
            else:
                if sys.argv[2] == 'rabbit':
                    get_rabbit_configuration(cfg, 5)
                elif sys.argv[2] == 'zoo':
                    get_zoo_configuration(cfg, 5)

            # try:
            #     rabbit_config(cfg)
            # except Exception as e:
            #     print u'Something wrong. Please ckeck the log'
            #     logging.info('Error: %s %s' % (e.msg,  e.message))
    else:
        print u'Usage ./rcs.py {start|config {rabbit|zoo}}'
    logging.info('stop %s' % cfg.app_name)