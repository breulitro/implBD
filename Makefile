FLAGS = `pkg-config --cflags --libs glib-2.0` -lreadline -lpthread

all:
	gcc -Wall main.c http.c restfullapi.c btree.c bufmgmt.c -g $(FLAGS) -o sgbd
	rm -f .datafile

debug:
	gcc -Wall main.c http.c restfullapi.c btree.c bufmgmt.c -g $(FLAGS) -o sgbd -DDEBUG
	rm -f .datafile

btest:
	gcc -Wall main.c http.c restfullapi.c btree.c bufmgmt.c -g $(FLAGS) -o sgbd -DBTEST
	rm -f .datafile

clean:
	rm -f sgbd .datafile
