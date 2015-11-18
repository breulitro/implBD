all:
	gcc -Wall main.c http.c restfullapi.c btree.c bufmgmt.c -g `pkg-config --cflags --libs glib-2.0` -lreadline -o sgbd -DDEBUG
	rm -f .datafile

iall:
	gcc -Wall bufmgmt.c -g `pkg-config --cflags --libs glib-2.0` -lreadline -o sgbd -std=c99
	rm -f .datafile


clean:
	rm -f sgbd .datafile
