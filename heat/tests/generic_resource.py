# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

from heat.engine import resource
from heat.engine import signal_responder

from heat.openstack.common import log as logging
from heat.openstack.common.gettextutils import _

logger = logging.getLogger(__name__)


class GenericResource(resource.Resource):
    '''
    Dummy resource for use in tests
    '''
    properties_schema = {}
    attributes_schema = {'foo': 'A generic attribute',
                         'Foo': 'Another generic attribute'}

    def handle_create(self):
        logger.warning(_('Creating generic resource (Type "%s")') %
                       self.type())

    def handle_update(self, json_snippet, tmpl_diff, prop_diff):
        logger.warning(_('Updating generic resource (Type "%s")') %
                       self.type())

    def handle_delete(self):
        logger.warning(_('Deleting generic resource (Type "%s")') %
                       self.type())

    def _resolve_attribute(self, name):
        return self.name

    def handle_suspend(self):
        logger.warning(_('Suspending generic resource (Type "%s")') %
                       self.type())

    def handle_resume(self):
        logger.warning(_('Resuming generic resource (Type "%s")') %
                       self.type())


class ResourceWithProps(GenericResource):
        properties_schema = {'Foo': {'Type': 'String'}}


class ResourceWithRequiredProps(GenericResource):
        properties_schema = {'Foo': {'Type': 'String',
                                     'Required': True}}


class SignalResource(signal_responder.SignalResponder):
    properties_schema = {}
    attributes_schema = {'AlarmUrl': 'Get a signed webhook'}

    def handle_signal(self, details=None):
        logger.warning(_('Signaled resource (Type "%(type)s") %(details)s')
                       % {'type': self.type(), 'details': details})

    def _resolve_attribute(self, name):
        if name == 'AlarmUrl' and self.resource_id is not None:
            return unicode(self._get_signed_url())
