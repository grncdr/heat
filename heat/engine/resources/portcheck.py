# vim: tabstop=4 shiftwidth=4 softtabstop=4

#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from datetime import datetime
from datetime import timedelta
import socket

import eventlet

from heat.common import exception
from heat.engine import resource

from heat.openstack.common import log

logger = log.getLogger(__name__)


class PortCheck(resource.Resource):
    properties_schema = {
        'Host': {'Type': 'String',
                 'Required': True},
        'Port': {'Type': 'Number',
                 'Required': True},
        'Timeout': {'Type': 'Number',
                    'Required': True}
    }

    def validate(self):
        res = super(PortCheck, self).validate()
        if res:
            return res

        if not (0 < self.properties['Port'] < 65536):
            return {'Error': 'Port must be a valid TCP port number'}

    def handle_create(self):
        host = self.properties['Host']
        port = self.properties['Port']
        expiry = datetime.now() + timedelta(seconds=self.properties['Timeout'])
        while datetime.now() < expiry:
            try:
                eventlet.connect((host, port)).close()
                return
            except socket.error:
                eventlet.sleep(5)

        raise exception.Error('Timed out waiting for %s:%s' % (host, port))

    def handle_update(self):
        return self.UPDATE_REPLACE


def resource_mapping():
    return {'OS::Heat::PortCheck': PortCheck}
