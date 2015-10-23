all:
	gcc bufmgmt.c -g `pkg-config --cflags --libs glib-2.0` -lreadline -lncurses -o sgbd
	rm -f .datafile
debug:
	gcc bufmgmt.c -g `pkg-config --cflags --libs glib-2.0` -lreadline -lncurses -o sgbd -DDEBUG
	rm -f .datafile
stuff:
	gcc `pkg-config --cflags --libs glib-2.0` create_datafile.c
	rm .datafile
