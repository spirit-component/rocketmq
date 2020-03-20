rocketmq


config example

```hocon

components.rocketmq.endpoint {
    cluster-a = {
        credentials = {
            access-key = "Your Access Key"
            secret-key = "Your Secret Key"
            channel    = "ALIYUN/OtherChannel"
        }

        subscribes = {
            todo-task-new = {
                name-server    = "http://127.0.0.1:9876"
                group-id       = "GROUP_ID_COMPONENT"
                topic          = "NewTodoTask"
                expression     = "TagA||TagB"
                message-model  = "clustering"   // broadcasting
                consumer-model = "cocurrently"  // orderly
            }

            todo-task-get  = {
                name-server = "http://127.0.0.1:9876"
                group-id    = "GROUP_ID_COMPONENT"
                topic       = "GetTodoTask"
                expression  = "*"
            }
        }
    }
}

```