rocketmq


config example

```hocon

components.rocketmq.endpoint {

        credentials = {
            c1 = {
                access-key = "Your Access Key"
                secret-key = "Your Secret Key"
                channel    = "ALIYUN/OtherChannel"
            }
        }

        consumer {
            credential-name = "c1"
            mode            = "pull"
            name-server     = "http://127.0.0.1:9876"
            group-id        = "GROUP_ID_COMPONENT"
            message-model   = "clustering"   // broadcasting
            consumer-model  = "cocurrently"  // orderly

            subscribe = {
                topic          = "NewTodoTask"
                expression     = "TagA||TagB"
            }
        }
    }
}

```