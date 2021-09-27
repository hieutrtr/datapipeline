# POST -H "Content-Type: application/vnd.schemaregistry.v1+json"
# --data
# '{ "schema": "[ { \"type\":\"record\", \"name\":\"countInfo\", \"fields\":
# [ {\"name\":\"count\",\"type\":\"long\"}]} ]" }' -u
# "$SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO" 
# "$SCHEMA_REGISTRY_URL/subjects/test2-value/versions"
import requests, json

class KafkaRest:
    def __init__(self, config):
        self.host = config['rest_host']
        self.schemaregistry_host = config['schemaregistry_host']
        self.schemaregistry_user = config['schemaregistry_user']
        self.schemaregistry_pass = config['schemaregistry_pass']
        self.binance_trade_schema_id = int(config['binance_trade_schema_id'])

    def register_schema(self,  subject, schema):
        api_url = "{}/subjects/{}/versions".format(self.schemaregistry_host, subject)
        schema_value  = {
            "schema": "{}".format(json.dumps(schema)),
            "schemaType": "AVRO",
            "references": [
                {
                    "name": "com.acme.Referenced",
                    "subject":  "childSubject",
                    "version": 1
                }
            ]
        }
        print('schema {}'.format(schema_value))
        headers = {
            'Content-Type': 'application/vnd.schemaregistry.v1+json'
        }
        res = requests.post(
                    url=api_url,
                    data=schema_value,
                    headers=headers,
                    # auth=(self.schemaregistry_user, self.schemaregistry_pass)
                  )
        if res.status_code != 200:
            res.raise_for_status()
        schema = res.json()
        return schema[-1].id

    def get_schema_by_subject(self, subject, version='latest'):
        api_url = "{}/subjects/{}/versions/{}".format(self.schemaregistry_host, subject, version)
        headers = {
            'Accept': 'application/vnd.schemaregistry.v1+json'
        }
        res = requests.get(
            url=api_url,
            headers=headers,
        )
        if res.status_code == 404:
            return None
        if res.status_code != 200:
            res.raise_for_status()
        schema = res.json()
        return schema.id

    def _map_record(self, record):
        record['time'] = str(record['time'])
        return {"value": record}

    def produce(self,  topic, records):
        api_url = "{}/topics/{}".format(self.host, topic)
        headers = {
            'Content-Type': 'application/vnd.kafka.avro.v2+json',
            'Accept': 'application/vnd.kafka.v2+json'
        }
        data = {
            "value_schema_id": self.binance_trade_schema_id,
            "records": list(map(self._map_record, records))
        }
        print("produce data {}".format(json.dumps(data)))
        res = requests.post(
            url=api_url,
            headers=headers,
            # auth=(self.schemaregistry_user, self.schemaregistry_pass),
            data=json.dumps(data)
        )
        if res.status_code != 200:
            res.raise_for_status()
        return res.json()