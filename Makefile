.PHONY: clean jar tag outdated install deploy tree repl test

clean:
	rm -rf target
	rm -rf classes

jar: clean tag
	clojure -A:jar

outdated:
	clojure -M:outdated

tag:
	clojure -A:tag

install: jar
	clojure -A:install

deploy: jar
	clojure -A:deploy

tree:
	mvn dependency:tree

repl:
	clojure -A:dev -A:repl

test:
	clojure -M:test -e "(require 'kinetic.consumer-test) (clojure.test/run-tests 'kinetic.consumer-test)"

