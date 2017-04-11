# Copyright 2015 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Define API Snapshots."""

class Snapshot(object):
    def __init__(name, subscription, client=None):

        if client is None and subscription is None:
            raise TypeError("Pass only one of 'subscription' or 'client'.")

        if client is not None and subscription is not None:
            raise TypeError("Pass only one of 'subscription' or 'client'.")

        self.name = name
        self.subscription = subscription
        self._client = client or subscription.client
        self._project = self._client.project

    @classmethod
    def from_api_repr(cls, resource, client, subscriptions=None):
        """Factory:  construct a subscription given its API representation

        :type resource: dict
        :param resource: snapshot resource representation returned from the API.

        :type client: :class:`google.cloud.pubsub.client.Client`
        :param client: Client which holds credentials and project
                       configuration.

        :type subscriptions: dict
        :param subscriptions:
            (Optional) A Subscription to which this snapshot belongs. If not passed, the
            subscription will have a newly-created subscription. Must have the same topic
            as the snapshot.

        :rtype: :class:`google.cloud.pubsub.subscription.Subscription`
        :returns: Subscription parsed from ``resource``.
        """
        if subscriptions is None:
            subscriptions = {}
        subscription_path = resource['subscription']
        if topic_path == cls._DELETED_TOPIC_PATH:
            topic = None

    @property
    def project(self):
        """Project bound to the subscription."""
        return self._client.project

    @property
    def full_name(self):
        """Fully-qualified name used in subscription APIs"""
        return 'projects/%s/subscriptions/%s' % (self.project, self.name)

    @property
    def path(self):
        """URL path for the subscription's APIs"""
        return '/%s' % (self.full_name,)

    def _require_client(self, client):
        """Check client or verify over-ride.

        :type client: :class:`~google.cloud.pubsub.client.Client` or
                      ``NoneType``
        :param client: the client to use.  If not passed, falls back to the
                       ``client`` stored on the topic of the
                       current subscription.

        :rtype: :class:`google.cloud.pubsub.client.Client`
        :returns: The client passed in or the currently bound client.
        """
        if client is None:
            client = self._client
        return client

    def create(self, client=None):
        """API call:  create the snapshot

        See:
        https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.snapshots/create

        :type client: :class:`~google.cloud.pubsub.client.Client` or
                      ``NoneType``
        :param client: the client to use.  If not passed, falls back to the
                       ``client`` stored on the current subscription's topic.
        """
        client = self._require_client(client)
        api = client.subscriber_api
        api.snapshot_create(self.full_name, self.subscription.full_name)

    # Note: no get so no exists() method

    def delete(self, client=None):
        """API call:  delete the snapshot

        See:
        https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.snapshots/delete

        :type client: :class:`~google.cloud.pubsub.client.Client` or
                      ``NoneType``
        :param client: the client to use.  If not passed, falls back to the
                       ``client`` stored on the current subscription's topic.
        """
        client = self._require_client(client)
        api = client.subscriber_api
        api.snapshot_delete(self.full_name)
