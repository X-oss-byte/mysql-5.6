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
import subprocess

""" crashme_methods

    utility / standard functions used when executing crashme
    these methods are used by native mode / unittest cases

"""

def prepare_config(bot):
    """ Create the config file crash-me needs to execute """

    output_filename = f"{bot.system_manager.workdir}/crashme.cfg"

    # remove the existing configuration file to start fresh
    if os.path.exists(output_filename):
        logging.info(f"Removing {output_filename}")
        os.remove(output_filename)

    with open(output_filename,"w") as output_file:
        # don't support '+' for concatenation
        output_file.writelines("func_extra_concat_as_+=no\n")
        # new boost libraries are causing us to put these limits in, needs investigation
        output_file.writelines("max_text_size=1048576\n")
        output_file.writelines("where_string_size=1048576\n")
        output_file.writelines("select_string_size=1048576\n")
        output_file.flush()

def execute_crashme(test_cmd, test_executor, servers):
    """ Execute the commandline and return the result.
        We use subprocess as we can pass os.environ dicts and whatnot 

    """

    # prepare our config file
    bot = test_executor
    prepare_config(bot)


    output_filename = f"{bot.system_manager.workdir}/crashme.cfg"
    testcase_name = bot.current_testcase.fullname
    crashme_outfile = os.path.join(bot.logdir,'crashme.out')
    with open(crashme_outfile,'w') as crashme_output:
        crashme_cmd = f"{test_cmd} --config-file={output_filename}"
        bot.logging.info(f"Executing crash-me:  {crashme_cmd}")
        bot.logging.info("This may take some time.  Please be patient...")

        crashme_subproc = subprocess.Popen( crashme_cmd
                                          , shell=True
                                          , cwd=os.path.join(bot.system_manager.testdir, 'sql-bench')
                                          , env=bot.working_environment
                                          , stdout = crashme_output
                                          , stderr = subprocess.STDOUT
                                          )
        crashme_subproc.wait()
        retcode = crashme_subproc.returncode     

    with open(crashme_outfile,'r') as crashme_file:
        output = ''.join(crashme_file.readlines())
        bot.logging.debug(output)
    bot.logging.debug("crashme_retcode: %d" %(retcode))
    bot.current_test_retcode = retcode
    bot.current_test_output = output
    test_status = process_crashme_output(bot)
    return test_status, retcode, bot.current_test_output

def process_crashme_output(bot):
    if bot.current_test_retcode != 0:
        return
    output_data = bot.current_test_output.split('\n')
    if output_data[0].startswith('Using an array as a reference is deprecated'):
        file_name_idx = 6
    else:
        file_name_idx = 3
    infile_name = output_data[file_name_idx].split(':')[1].strip()
    output_data = None
    with open(infile_name, "r") as inf:
        inlines= inf.readlines()
        error_flag= False
        in_error_section = False
        # crash-me is quite chatty and we don't normally want to sift
        # through ALL of that stuff.  We do allow seeing it via --verbose
        if not bot.verbose:
            bot.current_test_output = ''
        for inline in inlines:
            if in_error_section and not inline.strip().startswith('#'):
                in_error_section = False
            if '=error' in inline:
                error_flag= True
                in_error_section= True
            if in_error_section:
                bot.current_test_output += inline
    if not error_flag:
        if not bot.verbose:
            bot.current_test_output = None
        return 'pass'
    return 'fail'



