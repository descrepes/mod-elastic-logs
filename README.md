# Shinken logs ElasticSearch storage

Shinken broker module used to :
- store Shinken logs in an ElasticSearch index


## Main features

This module intercepts Shinken logs and analyses each log to store it in ElasticSearch. The logs are then available for an application such as Kibana.


## Requirements
Use elasticsearch version > 2.0.0 and elasticsearch-curator > 3.4.0

```
   pip install elasticsearch>=2.0
   pip install elasticsearch-curator>=3.4
```


## Enabling Elastic logs

To use the elastic-logs module you must declare it in your broker configuration.
```
   define broker {
      ...

      modules    	 ..., elastic-logs

   }
```


The module configuration is defined in the file: `elastic-logs.cfg`.

Default configuration needs to be tuned up to your ElasticSearch configuration.

```
## Module:      elastic-logs
## Loaded by:   Broker
# Store the Shinken logs in Elasticsearch
#
define module {
    module_name     elastic-logs
    module_type     elastic-logs

    # ElasticSearch cluster connection
    # EXAMPLE
    # hosts	es1.example.net:9200,es2.example.net:9200,es3.example.net:9200
    hosts           localhost:9200

    # The prefix of the index where to store the logs.
    # There will be one indexe per day: shinken-YYYY.MM.DD
    index_prefix    shinken

    # The timeout connection to the ElasticSearch Cluster
    timeout         20

    # Commit period
    # Every commit_period seconds, the module stores the received logs in the index
    # Default is to commit every 60 seconds
    commit_period     60

    # Commit volume
    # The module commits at most commit_volume logs in the index at every commit period (bulk operation)
    # Default is 200 lines
    commit_volume     200

    # Logs rotation
    #
    # Remove indices older than the specified value
    # Value is specified as :
    # 1d: 1 day
    # 3m: 3 months ...
    # d = days, w = weeks, m = months, y = years
    # Default is 1 month
    max_logs_age    1m

    # Services filtering
    # Filter is declared as a comma separated list of items:
    # An item can be a regexp which is matched against service description (hostname/service)
    #  ^test*, matches all hosts which name starts with test
    #  /test*, matches all services which name starts with test
    #
    # An item containing : is a specific filter (only bi is supported currently)
    #  bi:>x, bi:>=x, bi:<x, bi:<=x, bi:=x to match business impact

    # default is to consider only the services which business impact is > 4
    # 3 is the default value for business impact if property is not explicitely declared
    # Default is only bi>4 (most important services)
    #services_filter bi:>4
}

```

