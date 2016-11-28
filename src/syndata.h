//
// Defines for generating synthetic data
//

#ifndef      __SYNDATA_H
#define      __SYNDATA_H

#define SYN_PATTERN_SIZE	131072		// 100 KB
#define SYN_SUFFIX_MAX		128		// the maximum length of a synthetic file suffix
#define SYN_DIR_FMT		"d%d_%d"	// format used to generate synthetic directory entries
#define SYN_FILE_FMT		"f%d.%s"	// format used to generate synthetic file entries

// A structure to manage a buffer full of synthetic data
typedef struct {
	int length;			// length of buffer
	char *buf;			// The actual pattern buffer
} SyndataBuffer;

// A structure to hold the description and limits for a Synthetic Data Tree specification 
typedef struct {
         int max_level;			// maximum levels in directory tree
         size_t max_dirs;		// maximum number of directories per level
         size_t max_files;		// maximum number of files per directory
         int start_files;		// the level at which to start populating directories with files
} SyndataTreeSpec;

// convenient for outsiders to mock up dummy types
typedef SyndataBuffer* SyndataBufPtr;

// Function Declarations
SyndataBuffer*  syndataCreateBuffer(char *pname);
SyndataBuffer*  syndataCreateBufferWithSize(char *pname, int length);
SyndataBuffer*  syndataDestroyBuffer(SyndataBuffer *synbuf);
int             syndataExists       (SyndataBuffer *synbuf);
int             syndataFill         (SyndataBuffer *synbuf, char *inBuf, int inBufLen);
SyndataTreeSpec* syndataGetTreeSpec(const char *path);	// returns the directory specification for a synthetic data tree
int 		syndataGetDirLevel(const char *path);	// returns the directory level (for synthetic data directory)
int 	        isSyndataPath(char *path);		// tells if a path is a synthetic data
int 	        isSyndataDir(const char *path);		// tells if a path is a synthetic data tree
int		syndataSetAttr(const char *path, struct stat *st, int *lvl, size_t fsize);	// sets the attributes of the synthetic data path

#endif	//__SYNDATA_H
