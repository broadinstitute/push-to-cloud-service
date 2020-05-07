# push-to-cloud-service

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
clj -m start
```

The tests are managed by `clj` as well and will be executed by 
 [`kaocha`](https://github.com/lambdaisland/kaocha) test runner. You could 
 run the following code to see an example:

 ```bash
 clj -Atest
 ```

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