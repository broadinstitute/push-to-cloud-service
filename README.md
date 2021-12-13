# push-to-cloud-service

![Tests on Pull Requests and Master](https://github.com/broadinstitute/push-to-cloud-service/workflows/Tests%20on%20Pull%20Requests%20and%20Master/badge.svg)

This is the repo for the WorkFlow Launcher's PTC service

## Quickstart

This project is managed by the vanilla Clojure CLI tools, make sure
 you have it installed. If not, follow [here](https://clojure.org/guides/getting_started)
 to install it or simply run the following if you are on macOS:

 ```bash
 brew tap clojure/tools
 brew install clojure/tools/clojure
 ```

You could then run `clj -m $namespace` to run a module with given namespace. e.g.

```bash
clojure -M --main ptc.start
```

## Development

Add new dependencies to `deps.edn`.
Add new modules under `src/`,
and new test cases under `test/`.

## Testing

The tests are managed by `clj` and will be executed by
[`kaocha`](https://github.com/lambdaisland/kaocha) test runner. You could
run the following code to see an example:

```bash
clojure -M:test unit
```

For integration testing, you need to connect to the VPN or be in the Broad
network and run:

```bash
clojure -M:test integration
```

For end-to-end testing, you need to connect to the VPN or be in the Broad
network and run:
```bash
 clojure -M:test e2e
```

The e2e test runs in the dev environment by default. To run in production:
```bash
export ZAMBONI_ACTIVEMQ_SERVER_URL="failover:ssl://vpicard-jms.broadinstitute.org:61616"
export ZAMBONI_ACTIVEMQ_QUEUE_NAME="wfl.broad.pushtocloud.enqueue.prod-test"
export ZAMBONI_ACTIVEMQ_DEAD_LETTER_QUEUE_NAME="wfl.broad.pushtocloud.enqueue.prod-test-dlq"
export ZAMBONI_ACTIVEMQ_SECRET_PATH="secret/dsde/gotc/prod/activemq/logins/zamboni"
export PTC_BUCKET_URL="gs://broad-aou-arrays-input"
export CROMWELL_URL="https://cromwell-aou.gotc-prod.broadinstitute.org"
export WFL_URL="https://aou-wfl.gotc-prod.broadinstitute.org"

clojure -M:test integration
```

There is another test target named `acl-in-production`.
That runs a test
that includes reading from and writing to
the production Google Cloud Storage buckets.
The `acl-in-production` test is disabled
because it runs in production
and can break the AllOfUs pipeline.
Do not run it while the AllOfUs pipeline is in use.

You can edit code to enable the test
after you've verified that no samples
are in the AllOfUs pipeline,
and run it like this:

```bash
clojure -M:test acl-in-production
```

The `acl-in-production` test verifies
that the permissions on the production AllOfUs buckets
and the production Cromwell are set properly.


## Build

To build an executable [Uber Jar with AOT compilation](https://clojure.org/guides/deps_and_cli#aot_compilation),
simply run:

```bash
clojure -M:build
```

The JAR produced by can be run with
`java -jar target/push-to-cloud-service.jar`, which will invoke the
main function in the `start.clj` namespace, which is currently the only
module with `:gen-class`.

### Code Style

We use [`cljfmt`](https://github.com/weavejester/cljfmt) for a
standard and consistent code style in this repo.

To lint the code, run:
```bash
clojure -M:lint
```

To format the code, run:
```bash
clojure -M:format
```

#### Logging
Logging is done with `clojure.tools.logging` (very similarly to WFL's logging).
The backend is Log4j 2, configured in `resources/log4j2.xml`.
Examples are in `test/unit/logging_test.clj`.

## Deployment

push-to-cloud-service is designed to be a [systemd user unit](https://wiki.archlinux.org/index.php/Systemd/User).
Once installed via the bundled install script, you can enable push-to-cloud via:

```bash
systemctl --user enable --now push-to-cloud.service
systemctl enable ptc-vault.service
```

More information on systemd usage can be found [here](https://wiki.archlinux.org/index.php/Systemd).

## Troubleshooting

Hornet has a runbook
for troubleshooting the AoU PushToCloud service
[here](https://broadinstitute.atlassian.net/wiki/spaces/GHConfluence/pages/2869428228/Push+to+Cloud+Runbook).
