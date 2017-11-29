all:
	gcc -o project2 lex.c main.c -lcassandra

clean:
	rm -f project2
