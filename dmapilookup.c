#include "pfutils.h"
int
main (int argc, char *argv[])
{

	int i, c, errflg = 0;
	int inodenum, spacemgnum;
	struct stat st;
	int dmarray[3];
	char hexbuf[128];
	char path[PATHSIZE_PLUS];

	/* getopt stuff */
	char **paths;
	int numpaths;

	extern char *optarg;
	extern int optind, optopt;

	/* parse the input parms */
	while ((c = getopt (argc, argv, "h")) != -1) {
		switch (c) {
		case 'h':
			/*help? why would I help you */
			printf ("dmapi_lookup -p path -H\n");
			return (0);
			break;
		case ':':
			fprintf (stderr, "Option -%c requires an operand\n", optopt);
			errflg++;
			break;

		default:
			return (0);
		}
	}

	if (errflg) {
		printf ("dmapi_lookup -p path -H\n");
		exit (2);
	}

	paths = argv + optind;
	numpaths = argc - optind;

	for (i = 0; i < numpaths; i++) {
		snprintf (path, PATHSIZE_PLUS, "%s", paths[i]);
		if (lstat (path, &st) == -1) {
			perror ("lstat");
			exit (-1);
		}

		dmarray[0] = 0;
		dmarray[1] = 0;
		dmarray[2] = 0;

		if (!S_ISDIR (st.st_mode)) {

			inodenum = st.st_ino;

			if (read_inodes (path, inodenum, inodenum + 1, dmarray) != 0) {
				fprintf (stderr, "read_inodes failed: %s", strerror (errno));
				exit (1);
			}

			else {
				memset (hexbuf, 0, sizeof (hexbuf));

				if (dmarray[0] != 0) {
					spacemgnum = 1;
				}

				if (spacemgnum > 0) {
					if (dmapi_lookup (path, dmarray, hexbuf) != 0) {
						// dmapi_lookup failed here 
						fprintf (stderr, "dmapi lookup failed: %s", strerror (errno));
						exit (1);
					}

					else {
						printf ("%s -- ", path);
						// dmapi_look successed here 
						if (dmarray[1] == 1) {
							printf ("1 %d PREMIGRATED %s\n", spacemgnum, hexbuf);
						}
						else if (dmarray[2] == 1) {
							printf ("0 %d MIGRATED %s\n", spacemgnum, hexbuf);
						}
						else {
							printf ("1 %d ONDISK\n", spacemgnum);
						}
					}
				}
			}
		}
	}

	return 0;
}
