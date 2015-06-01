#include <hdfs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv)
{
	hdfsFS	fs;
	const char *writePath = "/testfile.txt";
	hdfsFile writeFile;
	char *buffer = "Hello, World!\n";
	tSize num_written_bytes;

	if ((fs = hdfsConnect("hdfs://localhost:9000", 0)) == NULL) {
		perror("hdfsConnect failed");
		exit(-1);
	}
	writeFile = hdfsOpenFile(fs, writePath, O_WRONLY | O_CREAT,
			0, 0, 0);
	if (!writeFile) {
		fprintf(stderr, "Failed to open %s for writing!\n",
				writePath);
		exit(-1);
	}
	num_written_bytes = hdfsWrite(fs, writeFile, (void *)buffer,
			strlen(buffer) + 1);
	if (hdfsFlush(fs, writeFile)) {
		fprintf(stderr, "Failed to 'flush' %s\n", writePath);
		exit(-1);
	}
	hdfsCloseFile(fs, writeFile);
	return(0);
}
