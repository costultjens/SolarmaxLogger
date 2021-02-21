#!/usr/bin/python
# -* coding: utf-8 *-

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Developed 2009-2010 by Bernd Wurst <bernd@schokokeks.org> 
# for own use.
# Released to the public in 2012.


import socket
import datetime
import logging

log = logging.getLogger("solarmax")

# Constants - range of Inverters supported
inverter_types = {
    20210: {'description': 'SolarMax SM10MT2', },
}

query_types = ['KDY', 'KYR', 'KMT', 'KT0', 'IL1', 'IDC', 'PAC', 'PRL',
               'SYS', 'SAL', 'TNF', 'PAC', 'PRL', 'TKK', 'UL1', 'UDC',
               'ADR', 'TYP', 'PIN', 'MAC', 'CAC', 'KHR', 'EC00', 'EC01',
               'EC02', 'EC03', 'EC04', 'EC05', 'EC06', 'EC07', 'EC08',
               'BDN', 'SWV', 'DIN', 'LAN', 'SDAT', 'FDAT', 'KLD', 'KLM',
               'KLY']

status_codes = {
    20000 : 'No Communication', 20001 : 'Running', 20002 : 'Irradiance too low', 20003 : 'Startup', 20004 : 'MPP operation', 20006 : 'Maximum power',
    20007 : 'Temperature limitation', 20008 : 'Mains operation', 20009 : 'Idc limitation', 20010 : 'Iac limitation',
    20011 : 'Test mode', 20012 : 'Remote controlled', 20013 : 'Restart delay', 20014 : 'External limitation',
    20015 : 'Frequency limitation', 20016 : 'Restart limitation', 20017 : 'Booting', 20018 : 'Insufficient boot power',
    20019 : 'Insufficient power', 20021 : 'Uninitialized', 20022 : 'Disabled', 20023 : 'Idle', 20024 : 'Powerunit not ready',
    20050 : 'Program firmware', 20101 : 'Device error 101', 20102 : 'Device error 102', 20103 : 'Device error 103',
    20104 : 'Device error 104', 20105 : 'Insulation fault DC', 20106 : 'Insulation fault DC', 20107 : 'Device error 107',
    20108 : 'Device error 108', 20109 : 'Vdc too high', 20110 : 'Device error 110', 20111 : 'Device error 111', 20112 : 'Device error 112',
    20113 : 'Device error 113', 20114 : 'Ierr too high', 20115 : 'No mains', 20116 : 'Frequency too high', 20117 : 'Frequency too low',
    20118 : 'Mains error', 20119 : 'Vac 10min too high', 20120 : 'Device error 120', 20121 : 'Device error 121', 20122 : 'Vac too high',
    20123 : 'Vac too low', 20124 : 'Device error 124', 20125 : 'Device error 125', 20126 : 'Error ext. input 1', 20127 : 'Fault ext. input 2',
    20128 : 'Device error 128', 20129 : 'Incorr. rotation dir.', 20130 : 'Device error 130', 20131 : 'Main switch off', 20132 : 'Device error 132',
    20133 : 'Device error 133', 20134 : 'Device error 134', 20135 : 'Device error 135', 20136 : 'Device error 136', 20137 : 'Device error 137',
    20138 : 'Device error 138', 20139 : 'Device error 139', 20140 : 'Device error 140', 20141 : 'Device error 141', 20142 : 'Device error 142',
    20143 : 'Device error 143', 20144 : 'Device error 144', 20145 : 'df/dt too high', 20146 : 'Device error 146', 20147 : 'Device error 147',
    20148 : 'Device error 148', 20150 : 'Ierr step too high', 20151 : 'Ierr step too high', 20153 : 'Device error 153', 20154 : 'Shutdown 1',
    20155 : 'Shutdown 2', 20156 : 'Device error 156', 20157 : 'Insulation fault DC', 20158 : 'Device error 158', 20159 : 'Device error 159',
    20160 : 'Device error 160', 20161 : 'Device error 161', 20163 : 'Device error 163', 20164 : 'Ierr too high', 20165 : 'No mains',
    20166 : 'Frequency too high', 20167 : 'Frequency too low', 20168 : 'Mains error', 20169 : 'Vac 10min too high', 20170 : 'Device error 170',
    20171 : 'Device error 171', 20172 : 'Vac too high', 20173 : 'Vac too low', 20174 : 'Device error 174', 20175 : 'Device error 175',
    20176 : 'Error DC polarity', 20177 : 'Device error 177', 20178 : 'Device error 178', 20179 : 'Device error 179', 20180 : 'Vdc too low',
    20181 : 'Blocked external', 20185 : 'Device error 185', 20186 : 'Device error 186', 20187 : 'Device error 187', 20188 : 'Device error 188',
    20189 : 'L and N interchanged', 20190 : 'Below-average yield', 20191 : 'Limitation error', 20198 : 'Device error 198', 20199 : 'Device error 199',
    20999 : 'Device error 999'
}

alarm_codes = {
    0: 'No Error',
    1: 'External Fault 1',
    2: 'Insulation fault DC side',
    4: 'Earth fault current too large',
    8: 'Fuse failure midpoint Earth',
    16: 'External alarm 2',
    32: 'Long-term temperature limit',
    64: 'Error AC supply ',
    128: 'External alarm 4',
    256: 'Fan failure',
    512: 'Fuse failure ',
    1024: 'Failure temperature sensor',
    2048: 'Alarm 12',
    4096: 'Alarm 13',
    8192: 'Alarm 14',
    16384: 'Alarm 15',
    32768: 'Alarm 16',
    65536: 'Alarm 17',
}


class SolarMax(object):
    def __init__(self, host, port):
        self.__host = host
        self.__port = port
        self.__inverters = {}
        self.__socket = None
        self.__connected = False
        self.__allinverters = False
        self.__inverter_list = []
        self.__connect()

    def __repr__(self):
        return 'SolarMax[%s:%s / socket=%s]' % (self.__host, self.__port, self.__socket)

    def __str__(self):
        return 'SolarMax[%s:%s / socket=%s / inverters=%s]' % (
            self.__host, self.__port, self.__socket, self.inverters())

    def __disconnect(self):
        try:
            log.info("Closing open connection to {}:{}".format(self.__host, self.__port))
            self.__socket.shutdown(socket.SHUT_RDWR)
            self.__socket.close()
            del self.__socket
        except:
            pass
        finally:
            self.__connected = False
            self.__allinverters = False
            self.__socket = None

    def __del__(self):
        log.debug("Destructor called")
        self.__disconnect()

    def __connect(self):
        self.__disconnect()
        log.info("Establishing connection to {}:{}...".format(self.__host, self.__port))
        try:
            # Python 2.6
            # Socket-timeout: 5 secs
            self.__socket = socket.create_connection((self.__host, self.__port), 5)
            self.__connected = True
            log.info("Connected.")
        except:
            log.info("Connection to {}:{} failed, maybe it is night?".format(self.__host, self.__port))
            self.__connected = False
            self.__allinverters = False

    # Utility functions
    def hexval(self, i):
        return (hex(i)[2:]).upper()

    def checksum(self, s):
        total = 0
        for c in s:
            total += ord(c)
        h = self.hexval(total)
        while len(h) < 4:
            h = '0' + h
        return h

    def __receive(self):
        try:
            data = ''
            tmp = ''
            while True:
                tmp = self.__socket.recv(1)
                data += tmp
                if len(tmp) < 1 or tmp == '}':
                    break
                tmp = ''
            return data
        except:
            self.__allinverters = False
            return ""

    def __parse(self, answer):
        # convenience checks
        if answer[0] != '{' or answer[-1] != '}':
            raise ValueError('Malformed answer: %s' % answer)
        raw_answer = answer
        answer = answer[1:-1]
        checksum = answer[-4:]
        content = answer[:-4]
        # checksum
        if checksum != self.checksum(content):
            raise ValueError('Checksum error')

        (header, content) = content[:-1].split('|', 2)
        (inverter, fb, length) = header.split(';', 3)
        if fb != 'FB':
            raise ValueError('Answer not understood')
        # length
        length = int(length, 16)
        if length != len(raw_answer):
            raise ValueError('Length mismatch')

        inverter = int(inverter, 16)

        # With write access to the WR responds with 'C8'
        # if not content.startswith('64:'):
        #  raise ValueError('Inverter did not understand our query')

        content = content[3:]
        data = {}

        for item in content.split(';'):
            (key, value) = item.split('=')
            if key not in query_types:
                raise NotImplementedError("Don't know %s" % item)
            data[key] = value
        return (inverter, data)

    def connected(self):
        return self.__connected

    def host_ip(self):
        return self.__host

    def __build_query(self, id, values, qtype=100):
        qtype = self.hexval(qtype)
        if type(values) == list:
            for v in values:
                if v not in query_types:
                    raise ValueError('Unknown data type »' + v + '«')
            values = ';'.join(values)
        elif type(values) in [str, unicode]:
            pass
        else:
            raise ValueError('Value has unsupported type')

        querystring = '|' + qtype + ':' + values + '|'
        # Length enlarge: 2 x {(2), WR number (2), "FB" (2) two semicolon (2), length itself (2), checksum (4)
        l = len(querystring) + 2 + 2 + 2 + 2 + 2 + 4
        querystring = 'FB;%s;%s%s' % (self.hexval(id), self.hexval(l), querystring)
        querystring += self.checksum(querystring)
        return '{%s}' % querystring

    def __send_query(self, querystring):
        try:
            self.__socket.send(querystring)
        except socket.timeout:
            self.__allinverters = False
        except socket.error:
            self.__connected = False

    def query(self, id, values, qtype=100):
        log.debug("Building query")
        q = self.__build_query(id, values, qtype)
        log.debug("Sending query to host {} => {}".format(self.__host, q))
        self.__send_query(q)
        answer = self.__receive()
        log.debug("Answer: {}".format(answer))
        if answer:
            (inverter, data) = self.__parse(answer)
            for d in data.keys():
                data[d] = self.normalize_value(d, data[d])
            return inverter, data
        else:
            self.__allinverters = False

        if not self.__allinverters and not self.__detection_running:
            self.detect_inverters()
        elif not self.__connected:
            self.__connect()
        else:
            raise socket.timeout
        return None

    def normalize_value(self, key, value):
        if key in ['KDY', 'UL1', 'UDC']:
            return float(int(value, 16)) / 10
        elif key in ['IL1', 'IDC', 'TNF', ]:
            return float(int(value, 16)) / 100
        elif key in ['PAC', 'PIN', ]:
            return float(int(value, 16)) / 2
        elif key in ['SAL', ]:
            return int(value, 16)
        elif key in ['SYS', ]:
            (x, y) = value.split(',', 2)
            x = int(x, 16)
            y = int(y, 16)
            return (x, y)
        elif key in ['SDAT', 'FDAT']:
            (date, time) = value.split(',', 2)
            time = int(time, 16)
            return datetime.datetime(int(date[:3], 16), int(date[3:5], 16), int(date[5:], 16), time / 3600,
                                     (time % 3600) / 60, time % (3600 * 60))
        else:
            return int(value, 16)

    def write_setting(self, inverter, data):
        rawdata = []
        for key, value in data.iteritems():
            key = key.upper()
            if key not in query_types:
                raise ValueError('unknown type')
            value = self.hexval(value)
            rawdata.append('%s=%s' % (key, value))
        log.debug(self.query(inverter, ';'.join(rawdata), 200))

    def status(self, inverter):
        result = self.query(inverter, ['SYS', 'SAL'])
        if not result:
            return ('Offline', 'Offline')
        result = result[1]
        errors = []
        if result['SAL'] > 0:
            for (code, descr) in alarm_codes.iteritems():
                if code & result['SAL']:
                    errors.append(descr)

        status = status_codes[result['SYS'][0]]
        return (status, ', '.join(errors))

    def use_inverters(self, list_of):
        self.__inverter_list = list_of
        log.debug("Inverters to detect: {}".format(list_of))
        self.detect_inverters()

    def detect_inverters(self):
        self.__inverters = {}
        if not self.__connected:
            self.__connect()

        if self.__connected:
            self.__detection_running = True
            for inverter in self.__inverter_list:
                try:
                    log.debug("Searching for #{} (socket: {})".format(inverter, self.__socket))
                    (inverter, data) = self.query(inverter, ['ADR', 'TYP', 'PIN'])
                    if data['TYP'] in inverter_types.keys():
                        self.__inverters[inverter] = inverter_types[data['TYP']].copy()
                        self.__inverters[inverter]['power_installed'] = data['PIN']
                    else:
                        log.debug("Unknown inverter type: {} (ID #{})".format(data['TYP'], data['ADR']))
                except Exception as e:
                    log.debug("Inverter #{} not found: {}".format(inverter, e))
                    self.__allinverters = False
            self.__detection_running = False
            if len(self.__inverters) == len(self.__inverter_list):
                self.__allinverters = True
                log.info("Found all inverters:")
                for inverter in self.__inverters.keys():
                    log.info("#{}: {}".format(inverter, self.__inverters[inverter]))
            else:
                log.info("Not all inverters found, reconnecting!")
                self.__connect()

    def inverters(self):
        if not self.__allinverters:
            self.detect_inverters()
        return self.__inverters
