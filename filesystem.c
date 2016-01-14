#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>

#define MAX_FILENAME_LENGTH 100
#define BLOCK_NUMBER 65536
#define BLOCK_SIZE 2048
#define FILE_NUMBER 64

#define FILE_PATH "/home/nastya/File System/fs"

typedef struct fmeta_struct {
	char name[MAX_FILENAME_LENGTH]; //name
	int start;			//number of start block
	int size;			//size
	int isDir;			//is directory (or file)
	int isNotFree;			//is free
} fmeta;

typedef struct filesystem_struct {
	fmeta meta[FILE_NUMBER];

	// block[i] >= 0   number of next block, 
	//          == -1  eof,
	//          == -2  empty
	int block[BLOCK_NUMBER];
} filesystem;

filesystem fs;

// init clear fs in memory
void init() {
	// init meta
	int i = 0;
	int n = sizeof(fs.meta) / sizeof(fs.meta[0]);
	while (i < n) {
		memset(fs.meta[i].name, '\0', MAX_FILENAME_LENGTH);
		fs.meta[i].start = -1;
		fs.meta[i].size = 0;
		fs.meta[i].isDir = 0;
		fs.meta[i].isNotFree = 0;	
		i++;
	}
	
	// init blocks
	i = 0;
	while (i < BLOCK_NUMBER) {
		fs.block[i++] = -2;
	}
}

// load fs from file
void load() {
	FILE *f = fopen(FILE_PATH, "r");
	fread(fs.meta, sizeof(fmeta), FILE_NUMBER, f);
	fread(fs.block, sizeof(int), BLOCK_NUMBER, f);
	
	// fix empty blocks
	int i = 0;
	while (i < BLOCK_NUMBER) {
		if (fs.block[i] == 0)
			fs.block[i] = -2;
		i++;
	}

	fclose(f);
}

int createClear() {
	FILE *f = fopen(FILE_PATH, "w+");

	// place for file meta
	char *buf = (char *)malloc(sizeof(fmeta));
	memset(buf, '\0', sizeof(fmeta));
	int i = 0;
	while (i < FILE_NUMBER) {
		fwrite(buf, sizeof(fmeta), 1, f);
		i++;
	}
	free(buf);

	// place for blocks info
	buf = (char *)malloc(sizeof(int));
	memset(buf, '\0', sizeof(int));
	i = 0;
	while (i < BLOCK_NUMBER) {
		fwrite(buf, sizeof(int), 1, f);
		i++;
	}
	free(buf);

	// place for block data
	buf = (char *)malloc(BLOCK_SIZE);
	memset(buf, '\0', BLOCK_SIZE);
	i = 0;
	while (i < BLOCK_NUMBER) {
		fwrite(buf, BLOCK_SIZE, 1, f);
		i++;
	}
	free(buf);

	fclose(f);
	init();
	addFile("/", 0, 1);
}

// find first empty element in meta
int findEmptyMeta() {
	int i = 0;
	int n = sizeof(fs.meta) / sizeof(fs.meta[0]);
	while (i < n) {
		if (!fs.meta[i].isNotFree) 
			return i;
		i++;
	}
	return -1; // not found	
}

// find first empty element in block
int findEmptyBlock() {
	int i = 0;
	while (i < BLOCK_NUMBER) {
		if (fs.block[i] == -2)
			return i;
		i++;
	}
	return -1; // not found
}

// get meta by number 
fmeta *getMetaByNumber(int k) {
	return &fs.meta[k];
}

// write meta[k] to fs file
int writeMeta(int k) {
	FILE *f = fopen(FILE_PATH, "r+");
	fseek(f, k * sizeof(fmeta), SEEK_SET);
	fwrite(getMetaByNumber(k), sizeof(fmeta), 1, f); ////
	fclose(f);
	return 0;
}

// write block[]
int writeBlocks(fmeta *meta) {
	FILE *f = fopen(FILE_PATH, "r+");
	int i = meta->start;

	do {
		fseek(f, sizeof(fmeta) * FILE_NUMBER + sizeof(int) * i, SEEK_SET);
		fwrite(&fs.block[i], sizeof(int), 1, f);
		i = fs.block[i];
	}
	while (i != -1);

	fclose(f);
	return 0;
}

// write data
/*
int writeData(fmeta *meta, const char *data, int size) {
	if (size == 0) return 0;
	FILE *f = fopen(FILE_PATH, "r+");
	int i, k = 0;
	i = meta->start;
	int count = BLOCK_SIZE;
	int skip = sizeof(fmeta) * FILE_NUMBER + sizeof(int) * BLOCK_NUMBER; // meta & blocks
	while (i != -1) { // not eof
		int left = size - k * BLOCK_SIZE;
		if (left >= BLOCK_SIZE) 
			count = BLOCK_SIZE;
		else 
			count = left;
		fseek(f, skip + i * BLOCK_SIZE + off, SEEK_SET);
		fwrite(data + k * BLOCK_SIZE, 1, count, f);
		i = fs.block[i];
		k++;
	}
	meta->size = size;
	writeBlocks(meta);
	fclose(f);
	return 0;	
}*/

int writeData(fmeta *meta, const char *data, int size, int offset) {
	if (size == 0) return 0;
	FILE *f = fopen(FILE_PATH, "r+");

	int i = meta->start, j = offset / BLOCK_SIZE;
	while (j-- > 0) i = fs.block[i];
	
	int left = size;
	int count = 0;
	int off = offset % BLOCK_SIZE;

	int skip = sizeof(fmeta) * FILE_NUMBER + sizeof(int) * BLOCK_NUMBER; // meta & blocks

	while (left > 0) { // there is something to write		
		if (off + left > BLOCK_SIZE)
			count = BLOCK_SIZE - off;
		else
			count = left;

		fseek(f, skip + i * BLOCK_SIZE + off, SEEK_SET);
		fwrite(data + size - left, 1, count, f);

		if (off + count == BLOCK_SIZE) {
			if (fs.block[i] >= 0) i = fs.block[i];
			else {
				int k = findEmptyBlock();
				if (k == -1) return -1;
				fs.block[i] = k;
				fs.block[k] = -1;
				i = k;
			}
		}

		left = left - count;
		off = 0;
	}

	meta->size = size + offset;

	writeBlocks(meta);
	fclose(f);
	return size;	
}

// read data
int readData(fmeta *meta, char **data) {
	if (meta == NULL) return -1;
	FILE *f = fopen(FILE_PATH, "r");
	char *buf = NULL;
	int size = meta->size;
	buf = (char *)malloc(size);
	int i = meta->start, k = 0, count;
	int skip = sizeof(fmeta) * FILE_NUMBER + sizeof(int) * BLOCK_NUMBER; // meta & blocks
	while (i != -1) {
		int left = size - k * BLOCK_SIZE;
		if (left >= BLOCK_SIZE) 
			count = BLOCK_SIZE;
		else 
			count = left;
		fseek(f, skip + i * BLOCK_SIZE, SEEK_SET);
		fread(buf + k * BLOCK_SIZE, 1, count, f);
		i = fs.block[i];
		k++;
	}
	fclose(f);
	*data = buf;
	return size;

}

int readFile(fmeta *meta, char **buf, int size, int offset) {
	if (size == 0) return 0;

	int s = meta->size;
	if (offset > s) return 0;
	if (size < s) s = size;

	FILE *f = fopen(FILE_PATH, "r");

	int i = meta->start, j = offset / BLOCK_SIZE;
	while (j-- > 0) i = fs.block[i];
	
	int left = s;
	int count = 0;
	int off = offset % BLOCK_SIZE;

	char *data = (char *)malloc(s); 

	int skip = sizeof(fmeta) * FILE_NUMBER + sizeof(int) * BLOCK_NUMBER; // meta & blocks

	while (left > 0) { // there is something to read		
		if (off + left > BLOCK_SIZE)
			count = BLOCK_SIZE - off;
		else
			count = left;

		fseek(f, skip + i * BLOCK_SIZE + off, SEEK_SET);
		fread(data + s - left, 1, count, f);

		if (off + count == BLOCK_SIZE) {
			i = fs.block[i];
		}

		left = left - count;
		off = 0;
	}
	fclose(f);
	*buf = (char *)data;

	return s;	
}

int getFileMetaNumber(char *data, char *name, int size) {
	int i = 0;
	int n = size / sizeof(int);
	while (i < n) {
		if (strcmp(fs.meta[((int *)data)[i]].name, name) == 0)
			return ((int *)data)[i];
		i++;	
	}
	return -1;
}

//get meta by filepath
int getMeta(const char *path, fmeta **meta) {
	char *fpath = (char*)malloc(strlen(path));
	strcpy(fpath, path);
	printf("%s\n", fpath);

	if (fpath && strcmp("/", fpath) == 0) { //root
		*meta = getMetaByNumber(0); 	
		return 0;
	}

	fmeta *m = NULL;
	char *p;

	p = fpath;

	if (*p++ == '/')
		m = getMetaByNumber(0);
	else return -1;

	char *data, *s;
	char name[MAX_FILENAME_LENGTH];
	memset(name, '\0', MAX_FILENAME_LENGTH);

	int k = -1, size;
	
	while (p - fpath < strlen(fpath)) {
		if (m->size == 0)
			return -1;
		size = readData(m, &data);
		s = p;
		p = strchr(p, '/');
		if (p != NULL) {
			p = p + 1;
			strncpy(name, s, p - s - 1);			
		}
		else {
			strncpy(name, s, fpath + strlen(fpath) - s);
			p = fpath + strlen(fpath);		
		}
		k = getFileMetaNumber(data, name, size);
		if (k == -1) return -1;
		m = getMetaByNumber(k);
		memset(name, '\0', MAX_FILENAME_LENGTH);
		free(data);
	}

	*meta = m;
	return k;
}

// add file to fs; returns number of meta
int addFile(char* name, int size, int isDir) {
	fmeta *meta = NULL;
	int k = findEmptyMeta();
	if (k == -1) return -1;
	meta = getMetaByNumber(k);
	int start = findEmptyBlock();
	if (start == -1) return -1;

	// write info to meta
	strcpy(meta->name, name);
	meta->start = start;
	meta->size = size;
	meta->isDir = isDir;
	meta->isNotFree = 1;

	fs.block[start] = -1;
	writeMeta(k);
	writeBlocks(meta);
	return k;
}

int createFile(const char* path, int isDir) {
	fmeta *meta;
	char *dir, *name;
	char *data, *moddata;

	name = strrchr(path, '/');
	if (name == NULL) {
		strcpy(name, path);
		dir = (char *)malloc(2);
		strcpy(dir, "/\0");
	} else {
		name = name + 1;
		int len = strlen(path) - strlen(name) ;
		dir = (char *)malloc(len + 1);
		strncpy(dir, path, len);
		dir[len] = '\0';
	}
printf("%s \t%s\n", dir, name);

	int metaNum = getMeta(dir, &meta);

	int size = readData(meta, &data);

	moddata = (char*)malloc(size + sizeof(int));
	memcpy(moddata, data, size);

	int k = addFile(name, 0, isDir);
	((int*)moddata)[size/sizeof(int)] = k;

	writeData(meta, moddata, size + sizeof(int), 0);
	meta->size = size + sizeof(int);	
	writeMeta(metaNum);

	free(moddata);
	free(dir);

	return 0;
}

char *getDirPath(const char* path) {
	char *dir;
	char *p = strrchr(path, '/');
	if (p != NULL) {
		int len = strlen(path) - strlen(p);
		if (len != 0) {
			dir = (char *)malloc(len + 1);
			strncpy(dir, path, len);	
			dir[len] = '\0';
		} else {
			dir = (char *)malloc(2);
			strcpy(dir, "/\0");
		}
	} else {
		dir = (char *)malloc(2);
		strcpy(dir, "/\0");
	}
	return dir;
}

int removeFile(const char* path) {
	fmeta *fileMeta, *dirMeta;
	char *data, *moddata;
	char *dir = getDirPath(path);

printf("removeFile: dir = %s\t path = %s\n", dir, path);

	int dirMetaNum = getMeta(dir, &dirMeta);
	int fMetaNum = getMeta(path, &fileMeta);
	int size = readData(dirMeta, &data);

	moddata = (char *)malloc(size - sizeof(int));
	int i = 0, j = 0;
	while (i < size / sizeof(int)) {
		if (((int *)data)[i] != fMetaNum)
			((int *)moddata)[j++] = ((int *)data)[i];
		i++; 
	}

	writeData(dirMeta, moddata, size, 0);
	dirMeta->size = size - sizeof(int);
	writeMeta(dirMetaNum);

	free(data);
	free(dir);

	return 0;
}

int openFile(const char* path) {
	fmeta *meta;
	int metaNum = getMeta(path, &meta);
	if (metaNum == -1) return -1;
	return 0;
}

// ============= FUSE OPERATIONS =============

static int fs_getattr(const char* path, struct stat *stbuf) {
	int res = 0;

	fmeta *meta;
	if (getMeta(path, &meta) == -1)
		res = -ENOENT;

	memset(stbuf, 0, sizeof(struct stat));
    	if(meta->isDir) {
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
    	}
    	else {
        	stbuf->st_mode = S_IFREG | 0444;
        	stbuf->st_nlink = 1;
        	stbuf->st_size = meta->size;
    	}
	stbuf->st_mode = stbuf->st_mode | 0777;
    	return res;
}

static int fs_mkdir(const char* path, mode_t mode) {
	if (createFile(path, 1) != 0)
		return -1;
	return 0;
}

static int fs_create(const char *path, mode_t mode, struct fuse_file_info *finfo) {
	if (createFile(path, 0) != 0)
		return -1;
	return 0;
}

static int fs_rmdir(const char *path) {
	int res = removeFile(path);
	if (res != 0) return -1;
	return 0;
}

static int fs_unlink(const char *path) {
	int res = removeFile(path);
	if (res != 0) return -1;
	return 0;
}

static int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, 
		off_t offset, struct fuse_file_info *fi) {

    	filler(buf, ".", NULL, 0);
    	filler(buf, "..", NULL, 0);

	fmeta *meta;
	int metaNum = getMeta(path, &meta);
printf("fs_readdir: metaNum = %d\n", metaNum);
	if (metaNum == -1) return -ENOENT;

	char *data;
	int size = readData(meta, &data);
printf("fs_readdir: size = %d\n", size);

    	int i = 0, n = size/sizeof(int);
	while (i < n) {
printf("fs_readdir: ((int*)data)[%d] = %d\nname = %s\n", i, ((int*)data)[i], fs.meta[((int*)data)[i]].name);
		filler(buf, fs.meta[((int*)data)[i]].name, NULL, 0);
		i++;
	}

    	return 0;
}

static int fs_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi) {
	fmeta *meta;
	int metaNum = getMeta(path, &meta);
	if (metaNum == -1) return -ENOENT;
	char *data;
	int s = readFile(meta, &data, size, offset);
	if (s == -1) 
		return -ENOENT;
	memcpy(buf, data, s);
    	return s;
}

static int fs_open(const char *path, struct fuse_file_info *fi)
{
	if (openFile(path) == -1) return -ENOENT;
	return 0;
}

static int fs_opendir(const char *path, struct fuse_file_info *fi)
{
	if (openFile(path) == -1) return -ENOENT;
	return 0;
}

static int fs_write(const char *path, const char *buf, size_t nbytes, 
			off_t offset, struct fuse_file_info *fi) {
	fmeta *meta;
	int metaNum = getMeta(path, &meta);
	if (metaNum == -1) return -ENOENT;
	int s = writeData(meta, buf, nbytes, offset);
	if (s != -1) {
		writeMeta(metaNum);
		return s;
	}
	return -1;
}

static int fs_rename(const char* from, const char* to) {
	fmeta *meta, *dirMeta;

	int metaNum = getMeta(from, &meta);
	if (metaNum == -1) return -ENOENT;
	removeFile(from);

	char *dir, *name;
	name = strrchr(to, '/');
	if (name == NULL) {
		strcpy(name, to);
		dir = (char *)malloc(2);
		strcpy(dir, "/\0");
	} else {
		name = name + 1;
		int len = strlen(to) - strlen(name) ;
		dir = (char *)malloc(len + 1);
		strncpy(dir, to, len);
		dir[len] = '\0';
	}

	memset(meta->name, '\0', MAX_FILENAME_LENGTH);
	strncpy(meta->name, name, strlen(name));
	writeMeta(metaNum);

	char *data;
	char *moddata;

	int dirMetaNum = getMeta(dir, &dirMeta);
	int size = readData(dirMeta, &data);

	moddata = (char*)malloc(size + sizeof(int));
	memcpy(moddata, data, size);
	((int*)moddata)[size/sizeof(int)] = metaNum;

	writeData(dirMeta, moddata, size + sizeof(int), 0);
	dirMeta->size = size + sizeof(int);	
	writeMeta(dirMetaNum);

	return 0;
}

static void *fs_init(struct fuse_conn_info *fi) {
	load();
	return 0;
}

static struct fuse_operations fs_oper = {
	.getattr	= fs_getattr,
	.readdir	= fs_readdir,
	.mkdir		= fs_mkdir,
	.rmdir		= fs_rmdir,
	.opendir	= fs_opendir,
	.create		= fs_create,
	.open		= fs_open,
	.read		= fs_read,
	.write		= fs_write,
	.unlink		= fs_unlink,
	.rename		= fs_rename,
	.init		= fs_init,	
};

int main(int argc, char *argv[])
{
	if (argc > 1 && strcmp(argv[1], "-n") == 0)
		createClear();
	else
		return fuse_main(argc, argv, &fs_oper, NULL);
}