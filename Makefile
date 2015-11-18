all:
	gcc -Wall main.c http.c restfullapi.c btree.c bufmgmt.c -g `pkg-config --cflags --libs glib-2.0` -lreadline -lpthread -o sgbd
	rm -f .datafile

debug:
	gcc -Wall main.c http.c restfullapi.c btree.c bufmgmt.c -g `pkg-config --cflags --libs glib-2.0` -lreadline -lpthread -o sgbd -DDEBUG
	rm -f .datafile

clean:
	rm -f sgbd .datafile
