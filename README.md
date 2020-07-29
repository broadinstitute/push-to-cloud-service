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
clojure -m ptc.start
```

The tests are managed by `clj` as well and will be executed by
 [`kaocha`](https://github.com/lambdaisland/kaocha) test runner. You could
 run the following code to see an example:

 ```bash
 clojure -A:test unit
 ```

For integration testing, you need to connect to the VPN or be in the Broad
network and run:

```bash
 clojure -A:test integration
```

which will run the end to end test including uploading some files to the
testing Google Cloud Storage Bucket.

## Development

```bash
$ tree .
.
├── LICENSE
├── README.md
├── deps.edn
├── src
│   └── start.clj
└── test
    └── start_test.clj
```
The project structure is shown as above, you add new entries to `deps.edn`
to introduce a new dependency, add new modules to `src/` and implement new
test cases to `test/`.

## Build

To build an executable [Uber Jar with AOT compilation](https://clojure.org/guides/deps_and_cli#aot_compilation),
simply run:

```bash
$ clojure -A:build
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
clojure -A:lint
```

To format the code, run:
```bash
clojure -A:format
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
```

More information on systemd usage can be found [here](https://wiki.archlinux.org/index.php/Systemd).
