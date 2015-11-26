FLAGS = `pkg-config --cflags --libs glib-2.0` -lreadline -lpthread -std=c99 -D_XOPEN_SOURCE=700

all:
	gcc -Wall main.c http.c restapi.c btree.c bufmgmt.c -g $(FLAGS) -o sgbd
	gcc -Wall client.c -g $(FLAGS) -o sgbd-client
	rm -f .datafile

debug:
	gcc -Wall main.c http.c restapi.c btree.c bufmgmt.c -g $(FLAGS) -o sgbd -DDEBUG
	gcc -Wall client.c -g $(FLAGS) -o sgbd-client
	rm -f .datafile

btest:
	gcc -Wall main.c http.c restapi.c btree.c bufmgmt.c -g $(FLAGS) -o sgbd -DBTEST
	gcc -Wall client.c -g $(FLAGS) -o sgbd-client
	rm -f .datafile

clean:
	rm -rf sgbd* .datafile server.log
