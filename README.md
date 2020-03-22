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
            credential-name = "c1"
            mode            = "pull"
            name-server     = "http://127.0.0.1:9876"
            group-id        = "GROUP_ID_COMPONENT"
            max-fetch       = 30

            subscribe = {
                topic          = "NewTodoTask"
                expression     = "TagA||TagB"
            }
        }
    }
}

components.rocketmq.endpoint_2 {

        credentials = {
            c1 = {
                access-key = "Your Access Key"
                secret-key = "Your Secret Key"
                channel    = "ALIYUN/OtherChannel"
            }
        }

        consumer {
            credential-name = "c1"
            mode            = "push"
            name-server     = "http://127.0.0.1:9876"
            group-id        = "GROUP_ID_COMPONENT"
            
            message-model   = "clustering"   // broadcasting
            consumer-model  = "cocurrently"  // orderly
            
            thread-count       = 2
            msg-batch-max-size = 32
            max-cache-msg-size = 4mb

            subscribe = {
                topic          = "NewTodoTask"
                expression     = "TagA||TagB"
            }
        }
    }
}

```