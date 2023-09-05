#! /usr/bin/env python
# -*- mode: python; indent-tabs-mode: nil; -*-
# vim:expandtab:shiftwidth=2:tabstop=2:smarttab:
#
# Copyright (C) 2011 Patrick Crews
#
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

import os
import shutil

from lib.util.mysqlBaseTestCase import mysqlBaseTestCase

server_requirements = [['--innodb_file_per_table']]
servers = []
server_manager = None
test_executor = None
# we explicitly use the --no-timestamp option
# here.  We will be using a generic / vanilla backup dir
backup_path = None

class basicTest(mysqlBaseTestCase):

    def setUp(self):
        master_server = servers[0] # assumption that this is 'master'
        backup_path = os.path.join(master_server.vardir, '_xtrabackup')
        # remove backup path
        if os.path.exists(backup_path):
            shutil.rmtree(backup_path)
        os.mkdir(backup_path)

    def load_table(self, table_name, row_count, server):
        queries = [
            "INSERT INTO %s VALUES (%d, %d)" % (table_name, i, row_count)
            for i in range(row_count)
        ]
        retcode, result = self.execute_queries(queries, server)
        self.assertEqual(retcode, 0, msg=result)


    def test_ib_incremental(self):
        self.servers = servers
        if servers[0].type not in ['mysql', 'percona']:
            return
        innobackupex = test_executor.system_manager.innobackupex_path
        xtrabackup = test_executor.system_manager.xtrabackup_path
        master_server = servers[0] # assumption that this is 'master'
        backup_path = os.path.join(master_server.vardir, '_xtrabackup')
        output_path = os.path.join(master_server.vardir, 'innobackupex.out')
        exec_path = os.path.dirname(innobackupex)
        table_name = "`test`"

            # populate our server with a test bed
        queries = [
            f"DROP TABLE IF EXISTS {table_name}",
            f"CREATE TABLE {table_name} (`a` int(11) DEFAULT NULL, `number` int(11) DEFAULT NULL)  ENGINE=InnoDB DEFAULT CHARSET=latin1",
        ]
        retcode, result = self.execute_queries(queries, master_server)
        self.assertEqual(retcode, 0, msg = result)
        self.load_table(table_name, 100, master_server)

            # take a backup
        cmd = [
            innobackupex,
            f"--defaults-file={master_server.cnf_file}",
            "--user=root",
            f"--socket={master_server.socket_file}",
            f"--ibbackup={xtrabackup}",
            backup_path,
        ]
        cmd = " ".join(cmd)
        retcode, output = self.execute_cmd(cmd, output_path, exec_path, True)
        self.assertEqual(retcode,0,output)
        main_backup_path = self.find_backup_path(output) 

        self.load_table(table_name, 500, master_server)

            # Get a checksum for our table
        query = f"CHECKSUM TABLE {table_name}"
        retcode, orig_checksum = self.execute_query(query, master_server)
        self.assertEqual(retcode, 0, msg=result)
        logging = test_executor.logging
        logging.test_debug(f"Original checksum: {orig_checksum}")

        # Take an incremental backup
        inc_backup_path = os.path.join(backup_path,'backup_inc1')
        cmd = [
            innobackupex,
            f"--defaults-file={master_server.cnf_file}",
            "--user=root",
            f"--socket={master_server.socket_file}",
            "--incremental",
            f"--incremental-basedir={main_backup_path}",
            f"--ibbackup={xtrabackup}",
            backup_path,
        ]
        cmd = " ".join(cmd)
        retcode, output = self.execute_cmd(cmd, output_path, exec_path, True)
        inc_backup_path = self.find_backup_path(output)
        self.assertTrue(retcode==0,output)

        # shutdown our server
        master_server.stop()

            # prepare our main backup
        cmd = [
            innobackupex,
            f"--defaults-file={master_server.cnf_file}",
            "--user=root",
            f"--socket={master_server.socket_file}",
            "--apply-log",
            "--redo-only",
            "--use-memory=500M",
            f"--ibbackup={xtrabackup}",
            main_backup_path,
        ]
        cmd = " ".join(cmd)
        retcode, output = self.execute_cmd(cmd, output_path, exec_path, True)
        self.assertEqual(retcode,0,output)

            # prepare our incremental backup
        cmd = [
            innobackupex,
            f"--defaults-file={master_server.cnf_file}",
            "--user=root",
            f"--socket={master_server.socket_file}",
            "--apply-log",
            "--redo-only",
            "--use-memory=500M",
            f"--ibbackup={xtrabackup}",
            f"--incremental-dir={inc_backup_path}",
            main_backup_path,
        ]
        cmd = " ".join(cmd)
        retcode, output = self.execute_cmd(cmd, output_path, exec_path, True)
        self.assertTrue(retcode==0,output)

            # do final prepare on main backup
        cmd = [
            innobackupex,
            f"--defaults-file={master_server.cnf_file}",
            "--user=root",
            f"--socket={master_server.socket_file}",
            "--apply-log",
            "--use-memory=500M",
            f"--ibbackup={xtrabackup}",
            main_backup_path,
        ]
        cmd = " ".join(cmd)
        retcode, output = self.execute_cmd(cmd, output_path, exec_path, True)
        self.assertEqual(
            retcode, 0, msg=f"Command: {cmd} failed with output: {output}"
        )


        # remove old datadir
        shutil.rmtree(master_server.datadir)
        os.mkdir(master_server.datadir)

            # restore from backup
        cmd = [
            innobackupex,
            f"--defaults-file={master_server.cnf_file}",
            "--user=root",
            f"--socket={master_server.socket_file}",
            "--copy-back",
            f"--ibbackup={xtrabackup}",
            main_backup_path,
        ]
        cmd = " ".join(cmd)
        retcode, output = self.execute_cmd(cmd, output_path, exec_path, True)
        self.assertTrue(retcode==0, output)

        # restart server (and ensure it doesn't crash)
        master_server.start()
        self.assertTrue(master_server.ping(quiet=True), 'Server failed restart from restored datadir...')

            # Get a checksum for our table
        query = f"CHECKSUM TABLE {table_name}"
        retcode, restored_checksum = self.execute_query(query, master_server)
        self.assertEqual(retcode, 0, msg=result)
        logging.test_debug(f"Restored checksum: {restored_checksum}")

        self.assertEqual(
            orig_checksum,
            restored_checksum,
            msg=f"Orig: {orig_checksum} | Restored: {restored_checksum}",
        )
 

