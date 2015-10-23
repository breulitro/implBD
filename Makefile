all:
	gcc bufmgmt.c -g `pkg-config --cflags --libs glib-2.0` -lreadline -lncurses -o sgbd
	rm .datafile
debug:
	gcc bufmgmt.c -g `pkg-config --cflags --libs glib-2.0` -lreadline -lncurses -o sgbd -DDEBUG
	rm .datafile
stuff:
	gcc `pkg-config --cflags --libs glib-2.0` create_datafile.c
	rm .datafile
