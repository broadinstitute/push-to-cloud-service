# Monitor push-to-cloud JMS queues

Run this to count messages
pending on the AllOfUs JMS queues
in the `dev` deployment environment.

``` bash
clj -M:jms dev
```

Run this to count messages
pending on the AllOfUs JMS queues
in the `staging` deployment environment.

``` bash
clj -M:jms staging
```

Run this to count messages
pending on the AllOfUs JMS queues
in the `prod` deployment environment.

``` bash
clj -M:jms prod
```

You should see something like this.

``` clojure
tbl@wmf05-d86 ~/Broad/push-to-cloud-service/jms # clj -M:jms dev
{"wfl.broad.pushtocloud.enqueue.dev"     0,
 "wfl.broad.pushtocloud.enqueue.dev-dlq" 8}
tbl@wmf05-d86 ~/Broad/push-to-cloud-service/jms #
```

That is all.
