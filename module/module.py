#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (C) 2009-2015:
#    Gabes Jean, naparuba@gmail.com
#    Gerhard Lausser, Gerhard.Lausser@consol.de
#    Gregory Starck, g.starck@gmail.com
#    Hartmut Goebel, h.goebel@goebel-consult.de
#    Frederic Mohier, frederic.mohier@gmail.com
#    Alexandre Le Mao, alexandre.lemao@gmail.com
#
# This file is part of Shinken.
#
# Shinken is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Shinken is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Shinken.  If not, see <http://www.gnu.org/licenses/>.


"""
This class is for attaching an Elasticsearch cluster to a broker module.
It is one possibility for an exchangeable storage for log broks
"""

import os
import time
from datetime import date, datetime, timedelta
import re
import sys
import traceback

from elasticsearch import Elasticsearch,helpers,ElasticsearchException,TransportError
import curator

from shinken.objects.service import Service
from shinken.modulesctx import modulesctx

from .log_line import (
    Logline,
    LOGCLASS_INVALID
)
from shinken.basemodule import BaseModule
from shinken.objects.module import Module
from shinken.log import logger

from collections import deque

properties = {
    'daemons': ['broker',],
    'type': 'elastic-logs',
    'external': True,
    'phases': ['running'],
    }

# called by the plugin manager
def get_instance(plugin):
    logger.info("[elastic-logs] got an instance of ElasticLogs for module: %s", plugin.get_name())
    instance = ElasticLogs(plugin)
    return instance


CONNECTED = 1
DISCONNECTED = 2
SWITCHING = 3

class ElasticLogsError(Exception):
    pass

class ElasticLogs(BaseModule):

    def __init__(self, mod_conf):
        BaseModule.__init__(self, mod_conf)
        
        self.hosts = getattr(mod_conf, 'hosts', 'localhost:9200')
        logger.info('[elastic-logs] Hosts: %s', self.hosts)

        self.index_prefix = getattr(mod_conf, 'index_prefix', 'shinken')
        logger.info('[elastic-logs] Index: %s', self.index_prefix)

        self.timeout = getattr(mod_conf, 'timeout', '20')
        logger.info('[elastic-logs] Timeout: %s', self.timeout)

        self.commit_period = int(getattr(mod_conf, 'commit_period', '60'))
        logger.info('[elastic-logs] periodical commit period: %ds', self.commit_period)

        self.commit_volume = int(getattr(mod_conf, 'commit_volume', '200'))
        logger.info('[elastic-logs] periodical commit volume: %d lines', self.commit_volume)

        self.cluster_test_period = int(getattr(mod_conf, 'cluster_test_period', '0'))
        logger.info('[elastic-logs] periodical ES Cluster connection test period: %ds', self.cluster_test_period)

        max_logs_age = getattr(mod_conf, 'max_logs_age', '1m')
        maxmatch = re.match(r'^(\d+)([dwmy]*)$', max_logs_age)
        if not maxmatch:
            logger.error('[elastic-logs] Wrong format for max_logs_age. Must be <number>[d|w|m|y] or <number> and not %s' % max_logs_age)
            return None
        else:
            if not maxmatch.group(2):
                self.max_logs_age = int(maxmatch.group(1))
            elif maxmatch.group(2) == 'd':
                self.max_logs_age = int(maxmatch.group(1))
            elif maxmatch.group(2) == 'w':
                self.max_logs_age = int(maxmatch.group(1)) * 7
            elif maxmatch.group(2) == 'm':
                self.max_logs_age = int(maxmatch.group(1)) * 31
            elif maxmatch.group(2) == 'y':
                self.max_logs_age = int(maxmatch.group(1)) * 365
        logger.info('[elastic-logs] max_logs_age: %s', self.max_logs_age)

        self.is_connected = DISCONNECTED

        self.next_logs_rotation = time.time() + 5000
        
        self.services_cache = {}
        services_filter = getattr(mod_conf, 'services_filter', '')
        logger.info('[elastic-logs] services filtering: %s', services_filter)

        self.filter_service_description = None
        self.filter_service_criticality = None
        if services_filter:
            # Decode services filter
            services_filter = [s for s in services_filter.split(',')]
            for rule in services_filter:
                rule = rule.strip()
                if not rule:
                    continue
                logger.info('[elastic-logs] services filtering rule: %s', rule)
                elts = rule.split(':', 1)

                t = 'service_description'
                if len(elts) > 1:
                    t = elts[0].lower()
                    s = elts[1].lower()

                if t == 'service_description':
                    self.filter_service_description = rule
                    logger.info('[elastic-logs] services will be filtered by description: %s', self.filter_service_description)

                if t == 'bp' or t == 'bi':
                    self.filter_service_criticality = s
                    logger.info('[elastic-logs] services will be filtered by criticality: %s', self.filter_service_criticality)
        
        self.logs_cache = deque()

    def load(self, app):
        self.app = app

    def init(self):
        return True

    def open(self):
        """
        Connect to ES cluster.
        Execute a command to check if connected on master to activate immediate connection to
        the DB because we need to know if DB server is available.
        Update log rotation time to force a log rotation
        """
        logger.info("[elastic-logs] trying to connect to ES Cluster: %s", self.hosts)
	self.es = Elasticsearch(self.hosts.split(','), timeout=int(self.timeout))
        try:
	    self.es.cluster.health()
            logger.info("[elastic-logs] connected to the ES Cluster: %s", self.hosts)
            self.is_connected = CONNECTED
            self.next_logs_rotation = time.time()

        except TransportError, exp:
            logger.error("[elastic-logs] Cluster is not available: %s", str(exp))
	    self.is_connected = DISCONNECTED
            return False
        
        return True

    def close(self):
        self.is_connected = DISCONNECTED
        #connections.remove_connection('shinken')
        logger.info('[elastic-logs] Cluster connection closed')

    def commit(self):
        pass


    def manage_brok(self, brok):
        """
        Overloaded parent class manage_brok method:
        - select which broks management functions are to be called
        """
        manage = getattr(self, 'manage_' + brok.type + '_brok', None)
        if manage:
            return manage(brok)

    def manage_initial_host_status_brok(self, brok):
        start = time.clock()
        host_name = brok.data['host_name']
        service_description = ''
        service_id = host_name+"/"+service_description
        logger.debug("[elastic-logs] initial host status received: %s (bi=%d)", host_name, int (brok.data["business_impact"]))

        self.services_cache[service_id] = { "hostname": host_name, "service": service_description }
        logger.info("[elastic-logs] host registered: %s (bi=%d)", service_id, brok.data["business_impact"])

    def manage_host_check_result_brok(self, brok):
        start = time.clock()
        host_name = brok.data['host_name']
        service_description = ''
        service_id = host_name+"/"+service_description
        logger.debug("[elastic-logs] host check result received: %s", service_id)

        if self.services_cache and service_id in self.services_cache:
            #self.record_availability(host_name, service_description, brok)
            logger.debug("[elastic-logs] host check result: %s, %.2gs", service_id, time.clock() - start)
    
    def manage_initial_service_status_brok(self, brok):
        start = time.clock()
        host_name = brok.data['host_name']
        service_description = brok.data['service_description']
        service_id = host_name+"/"+service_description
        logger.debug("[elastic-logs] initial service status received: %s (bi=%d)", host_name, int (brok.data["business_impact"]))

        # Filter service if needed: reference service in services cache ...
        # ... if description filter matches ...
        if self.filter_service_description:
            pat = re.compile(self.filter_service_description, re.IGNORECASE)
            if pat.search(service_id):
                self.services_cache[service_id] = { "hostname": host_name, "service": service_description }
                logger.info("[elastic-logs] service description filter matches for: %s (bi=%d)", service_id, brok.data["business_impact"])

        # ... or if criticality filter matches.
        if self.filter_service_criticality:
            include = False
            bi = int (brok.data["business_impact"])
            if self.filter_service_criticality.startswith('>='):
                if bi >= int(self.filter_service_criticality[2:]):
                    include = True
            elif self.filter_service_criticality.startswith('<='):
                if bi <= int(self.filter_service_criticality[2:]):
                    include = True
            elif self.filter_service_criticality.startswith('>'):
                if bi > int(self.filter_service_criticality[1:]):
                    include = True
            elif self.filter_service_criticality.startswith('<'):
                if bi < int(self.filter_service_criticality[1:]):
                    include = True
            elif self.filter_service_criticality.startswith('='):
                if bi == int(self.filter_service_criticality[1:]):
                    include = True
            if include:
                self.services_cache[service_id] = { "hostname": host_name, "service": service_description }
                logger.info("[elastic-logs] criticality filter matches for: %s (bi=%d)", service_id, brok.data["business_impact"])

    def manage_service_check_result_brok(self, brok):
        start = time.clock()
        host_name = brok.data['host_name']
        service_description = brok.data['service_description']
        service_id = host_name+"/"+service_description
        logger.debug("[elastic-logs] service check result received: %s", service_id)

        if self.services_cache and service_id in self.services_cache:
            self.record_availability(host_name, service_description, brok)
            logger.debug("[elastic-logs] service check result: %s, %.2gs", service_id, time.clock() - start)

    def manage_log_brok(self, brok):
        """
        Parse a Shinken log brok to enqueue a log line for Index insertion
        """
        d = date.today()
        index_name = self.index_prefix + '-' + d.strftime('%Y.%m.%d')

        line = brok.data['log']
        if re.match("^\[[0-9]*\] [A-Z][a-z]*.:", line):
            # Match log which NOT have to be stored
            logger.warning('[elastic-logs] do not store: %s', line)
            return

        logline = Logline(line=line)
        logline_dict = logline.as_dict()
        logline_dict.update({'@timestamp': datetime.utcfromtimestamp(int(logline_dict['time'])).isoformat()+'Z'})
        values = {
                '_index': index_name,
                '_type': 'shinken-logs',
                '_source': logline_dict
        }

        #values = logline.as_dict()
        if logline.logclass != LOGCLASS_INVALID:
            logger.debug('[elastic-logs] store log line values: %s', values)
            self.logs_cache.append(values)
        else:
            logger.info("[elastic-logs] This line is invalid: %s", line)

        return


    def create_index(self,index):
        try:
            logger.debug("[elastic-logs] Creating index %s ...", index)
            self.es.indices.create(index)
        except ElasticsearchException, exp:
            logger.error("[elastic-logs] exception while creating index %s: %s", index, str(exp))

    def is_index_exists(self,index):
	if not self.is_connected == CONNECTED:
            try:
                if self.es.indices.exists(index):
                    return True
                else:
                    return False
            except ElasticsearchException, exp:
                logger.error("[elastic-logs] exception while checking the existance of the index %s: %s", index, str(exp))

	return True    

    def rotate_logs(self):
        """
        We will delete indices older than configured maximum age.
        """
        
        if not self.is_connected == CONNECTED:
            if not self.open():
                self.next_logs_rotation = time.time() + 600
                logger.info("[elastic-logs] log rotation failed, next log rotation at %s " % time.asctime(time.localtime(self.next_logs_rotation)))
                return

        logger.info("[elastic-logs] rotating logs ...")

        now = time.time()
        today = date.today()
        today0000 = datetime(today.year, today.month, today.day, 0, 0, 0)
        today0005 = datetime(today.year, today.month, today.day, 0, 5, 0)
        oldest = today0000 - timedelta(days=self.max_logs_age)
        if now < time.mktime(today0005.timetuple()):
            next_rotation = today0005
        else:
            next_rotation = today0005 + timedelta(days=1)

	try: 
            indices = curator.get_indices(self.es)
            filter_list = []
            filter_list.append(curator.build_filter(kindOf='prefix', value=self.index_prefix))
            filter_list.append(
                curator.build_filter(
                    kindOf='older_than', value=self.max_logs_age, time_unit='days',
                    timestring='%Y.%m.%d'
                )
            )
            working_list = indices
            for filter in filter_list:
                working_list = curator.apply_filter(working_list, **filter)

            curator.delete(self.es,working_list)        
            logger.info("[elastic-logs] removed %d logs older than %s days.", working_list, self.max_logs_age)

	except Exception, exp:
	    logger.error("[elastic-logs] Exception while rotating indices %s: %s", working_list, str(exp))

	if now < time.mktime(today0005.timetuple()):
            next_rotation = today0005
        else:
            next_rotation = today0005 + timedelta(days=1)

	self.next_logs_rotation = time.mktime(next_rotation.timetuple())
	logger.info("[elastic-logs] next log rotation at %s " % time.asctime(time.localtime(self.next_logs_rotation)))


    def commit_logs(self):
        """
        Periodically called (commit_period), this method prepares a bunch of queued logs (commit_colume) to insert them in the index
        """
        if not self.logs_cache:
            return

        if not self.is_connected == CONNECTED:
            if not self.open():
                logger.warning("[elastic-logs] log commiting failed")
                logger.warning("[elastic-logs] %d lines to insert in the index", len(self.logs_cache))
                return

        logger.debug("[elastic-logs] commiting ...")

        logger.debug("[elastic-logs] %d lines to insert in the index (max insertion is %d lines)", len(self.logs_cache), self.commit_volume)

        # Flush all the stored log lines
        logs_to_commit = 1
        now = time.time()
        some_logs = []
        while True:
            try:
                # result = self.db[self.logs_collection].insert_one(self.logs_cache.popleft())
                some_logs.append(self.logs_cache.popleft())
                logs_to_commit = logs_to_commit + 1
                if logs_to_commit >= self.commit_volume:
                    break
            except IndexError:
                logger.debug("[elastic-logs] prepared all available logs for commit")
                break
            except Exception, exp:
                logger.error("[elastic-logs] exception: %s", str(exp))
        
        logger.debug("[elastic-logs] time to prepare %s logs for commit (%2.4f)", logs_to_commit, time.time() - now)

        now = time.time()
        try:
            # Insert lines to commit
            result = helpers.bulk(self.es,some_logs,self.commit_volume)
            logger.debug("[elastic-logs] inserted %d logs.", result)

        except ElasticsearchException, exp:
            self.close()
            logger.error("[elastic-logs] Error occurred when commiting: %s", exp)

        logger.debug("[elastic-logs] time to insert %s logs (%2.4f)", logs_to_commit, time.time() - now)


    def main(self):
        self.set_proctitle(self.name)
        self.set_exit_handler()

        # Open database connection
        self.open()

        db_commit_next_time = time.time()
        db_test_connection = time.time()

        while not self.interrupted:
            logger.debug("[elastic-logs] queue length: %s", self.to_q.qsize())
            now = time.time()
            d = date.today()
            index_name = self.index_prefix + '-' + d.strftime('%Y.%m.%d')

            # DB connection test ?
            if self.cluster_test_period and db_test_connection < now:
                logger.debug("[elastic-logs] Testing cluster connection ...")
                # Test connection every 5 seconds ...
                db_test_connection = now + self.cluster_test_period
                if self.is_connected == DISCONNECTED:
                    self.open()

            # Create index ?
            if not self.is_index_exists(index_name):
                self.create_index(index_name)

            # Logs commit ?
            if db_commit_next_time < now:
                logger.debug("[elastic-logs] Logs commit time ...")
                # Commit periodically ...
                db_commit_next_time = now + self.commit_period
                self.commit_logs()

            # Logs rotation ?
            if self.next_logs_rotation < now:
                logger.debug("[elastic-logs] Logs rotation time ...")
                self.rotate_logs()

            # Broks management ...
            l = self.to_q.get()
            for b in l:
                b.prepare()
                self.manage_brok(b)

            logger.debug("[elastic-logs] time to manage %s broks (%3.4fs)", len(l), time.time() - now)

        # Close cluster connection
        self.close()
