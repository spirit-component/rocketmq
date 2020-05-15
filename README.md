rocketmq


config example

```hocon

components.rocketmq.endpoint_1 {

        credentials = {
            c1 = {
                access-key = "Your Access Key"
                secret-key = "Your Secret Key"
                channel    = "ALIYUN/OtherChannel"
            }
        }

        consumer {

            rate-limit {
                qps = 1000
                bucket-size = 1
            }

            credential-name = "c1"
            mode            = "pull"
            name-server     = "http://127.0.0.1:9876"
            group-id        = "GROUP_ID_COMPONENT"
            max-fetch       = 30

            subscribe = {
                topic          = "API"
                expression     = "*"     // https://rocketmq.apache.org/docs/filter-by-sql92-example/
                queue-table  {
                    provider  = in-memory
                    queue-ids = [0,1,2,3]
                }
            }
        }
    }
}

```