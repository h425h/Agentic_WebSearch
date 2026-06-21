CP = lib/*
SRC_CP = out:lib/*

all: webserver kvs flame crawler indexer frontend

webserver:
	@mkdir -p out
	javac -cp "$(CP)" -d out src/cis5550/webserver/*.java src/cis5550/tools/*.java

kvs: webserver
	javac -cp "$(SRC_CP)" -d out src/cis5550/kvs/*.java src/cis5550/tools/*.java

flame: webserver
	javac -cp "$(SRC_CP)" -d out src/cis5550/flame/*.java src/cis5550/tools/*.java

crawler: webserver
	@mkdir -p out bin
	javac -cp "$(SRC_CP)" -d out src/cis5550/crawler/*.java src/cis5550/tools/*.java
	jar cf bin/crawler.jar -C out .

indexer: webserver
	@mkdir -p out bin
	javac -cp "$(SRC_CP)" -d out src/cis5550/indexer/*.java src/cis5550/tools/*.java src/cis5550/external/*.java
	jar cf bin/indexer.jar -C out .

frontend: webserver
	javac -cp "$(SRC_CP)" -d out src/cis5550/frontend/*.java src/cis5550/tools/*.java src/cis5550/external/*.java

clean:
	rm -rf out bin
