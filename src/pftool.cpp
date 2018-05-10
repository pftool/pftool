// -*- mode: c++; c-basic-offset: 4 -*-
/*
 *This material was prepared by the Los Alamos National Security, LLC (LANS) under
 *Contract DE-AC52-06NA25396 with the U.S. Department of Energy (DOE). All rights
 *in the material are reserved by DOE on behalf of the Government and LANS
 *pursuant to the contract. You are authorized to use the material for Government
 *purposes but it is not to be released or distributed to the public. NEITHER THE
 *UNITED STATES NOR THE UNITED STATES DEPARTMENT OF ENERGY, NOR THE LOS ALAMOS
 *NATIONAL SECURITY, LLC, NOR ANY OF THEIR EMPLOYEES, MAKES ANY WARRANTY, EXPRESS
 *OR IMPLIED, OR ASSUMES ANY LEGAL LIABILITY OR RESPONSIBILITY FOR THE ACCURACY,
 *COMPLETENESS, OR USEFULNESS OF ANY INFORMATION, APPARATUS, PRODUCT, OR PROCESS
 *DISCLOSED, OR REPRESENTS THAT ITS USE WOULD NOT INFRINGE PRIVATELY OWNED RIGHTS.
 */

//Standard includes
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <fcntl.h>
#include <signal.h>             // timers
#include <time.h>
#include <syslog.h>
#include <sys/types.h>
#include <pwd.h>
#include <fnmatch.h>

#include "pftool.h"
#include "ctm.h"
#include "Path.h"

#include <map>
#include <string>
#include <vector>
using namespace std;

int main(int argc, char *argv[]) {
    //general variables
    int i;
    //mpi
    int rank = 0;
    int nproc = 0;
    //getopt
    int c;
    struct options o;
    //queues
    path_list *input_queue_head = NULL;
    path_list *input_queue_tail = NULL;
    int        input_queue_count = 0;
    //paths
    char        src_path[PATHSIZE_PLUS];
    char        dest_path[PATHSIZE_PLUS];
    // should we run (this allows for a clean exit on -h)
    int ret_val = 0;
    int run = 1;

#ifdef S3
    // aws_init() -- actually, curl_global_init() -- is supposed to be
    // called before *any* threads are created.  Could MPI_Init() create
    // threads (or call multi-threaded libraries)?  We'll assume so.
    AWS4C_CHECK( aws_init() );
    s3_enable_EMC_extensions(1);
#endif

#ifdef MARFS
    // aws_init() (actually, curl_global_init()) is supposed to be done
    // before *any* threads are created.  Could MPI_Init() create threads
    // (or call multi-threaded libraries)?  We'll assume so.
    AWS4C_CHECK( aws_init() );
    //s3_enable_EMC_extensions(1); TODO: Where does this need to be set
    //
   
   {
      int rootEscalation;
      rootEscalation = 0;

      if(0 == geteuid()) {
         rootEscalation = 1;
      }

      char* const user_name = (char*)"root";
      if (aws_read_config(user_name)) {
          fprintf(stderr, "unable to load AWS4C config\n");
          exit(1);
      } 

      if(1 == rootEscalation) {
         if (0 != seteuid(getuid())) {
            perror("unable to set euid back to user");
            exit(1);
         }
         // probably a no-op, unless someone accidentally sets SGID on pftool.
         if (0 != setegid(getgid())) {
            perror("unable to set egid back to user");
            exit(1);
         }
      }
   }

   if (read_configuration()) {
       fprintf(stderr, "unable to load MarFS config\n");
       exit(1);
   }
   if (validate_configuration()) {
       fprintf(stderr, "MarFS config failed validation-tests\n");
       exit(1);
   }


    init_xattr_specs();

# ifdef USE_SPROXYD
    // NOTE: sproxyd doesn't require authentication, and so it could work on
    //     an installation without a ~/.awsAuth file.  But suppose we're
    //     supporting some repos that use S3 and some that use sproxyd?  In
    //     that case, the s3 requests will need this.  Loading it once
    //     up-front, like this, at start-time, means we don't have to reload
    //     it inside marfs_open(), for every S3 open, but it also means we
    //     don't know whether we really need it.
    //
    // ALSO: At start-up time, $USER is "root".  If we want per-user S3 IDs,
    //     then we would have to either (a) load them all now, and
    //     dynamically pick the one we want inside marfs_open(), or (b) call
    //     aws_read_config() inside marfs_open(), using the euid of the user
    //     to find ~/.awsAuth.
    int config_fail_ok = 1;
# else
    int config_fail_ok = 0;
# endif

#endif

    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        fprintf(stderr, "Error in MPI_Init\n");
        return -1;
    }

    // Get the number of procs
    if (MPI_Comm_size(MPI_COMM_WORLD, &nproc) != MPI_SUCCESS) {
        fprintf(stderr, "Error in MPI_Comm_size\n");
        return -1;
    }
    // Get our rank
    if (MPI_Comm_rank(MPI_COMM_WORLD, &rank) != MPI_SUCCESS) {
        fprintf(stderr, "Error in MPI_Comm_rank\n");
        return -1;
    }

    //Process using getopt
    //initialize options
    if (rank == MANAGER_PROC) {
        memset((void*)&o, 0, sizeof(struct options));
        o.verbose = 0;
        o.debug = 0;
        o.use_file_list = 0;
        o.recurse = 0;
        o.logging = 0;
        o.meta_data_only = 1;
        o.dest_fstype = UNKNOWN_FS;
        strncpy(o.jid, "TestJob", 128);
        o.parallel_dest = 0;
        o.blocksize = (1024 * 1024);
        o.chunk_at  = (10ULL * 1024 * 1024 * 1024); // 10737418240
        o.chunksize = (10ULL * 1024 * 1024 * 1024);
        strncpy(o.exclude, "", PATHSIZE_PLUS);
        o.max_readdir_ranks = MAXREADDIRRANKS;
        src_path[0] = '\0';
        dest_path[0] = '\0';

        // marfs can't default these until we see the destination
        bool chunk_at_defaulted = true;
        bool chunk_sz_defaulted = true;

        o.work_type = LSWORK;   // default op is to do a listing (not printed)

#ifdef GEN_SYNDATA
        o.syn_pattern[0] = '\0'; // Make sure synthetic data pattern file or name is clear
        o.syn_size = 0;          // Clear the synthetic data size
        o.syn_suffix[0] = '\0';  // Clear the synthetic data suffix
#endif

        // start MPI - if this fails we cant send the error to thtooloutput proc so we just die now
        while ((c = getopt(argc, argv, "p:c:j:w:i:s:C:S:a:f:d:W:A:t:X:x:z:e:D:orlPMnhvg")) != -1) {
            switch(c) {
            case 'p':
                //Get the source/beginning path
                strncpy(src_path, optarg, PATHSIZE_PLUS);
                break;
            case 'c':
                //Get the destination path
                strncpy(dest_path, optarg, PATHSIZE_PLUS);
                break;
            case 'j':
                strncpy(o.jid, optarg, 128);
                break;
            case 't':
                o.dest_fstype = Path::parse_fstype(optarg);
                break;
            case 'w':
                // this is <WorkType>, from pfutils.h
                // 0 = copy, 1 = list, 2 = compare
                o.work_type = atoi(optarg);
                break;
            case 'i':
                strncpy(o.file_list, optarg, PATHSIZE_PLUS);
                o.use_file_list = 1;
                break;
            case 's':
                o.blocksize = str2Size(optarg);
                break;
            case 'C':
                o.chunk_at = str2Size(optarg);
                break;
            case 'S':
                o.chunksize = str2Size(optarg);
                break;
            case 'D':
                o.max_readdir_ranks = atoi(optarg);
                break;
            case 'X':
#ifdef GEN_SYNDATA
                strncpy(o.syn_pattern, optarg, 128);
#else
                errsend(NONFATAL,"configure with --enable-syndata, to use option '-X'");
#endif
                break;
            case 'x':
#ifdef GEN_SYNDATA
                int slen;
                o.syn_size = str2Size(optarg);
                strncpy(o.syn_suffix,optarg,SYN_SUFFIX_MAX-2); // two less, so that we can add a 'b' if needed

                // if last character is a digit -> add a 'b' for bytes
                if (isdigit(o.syn_suffix[(slen=strlen(o.syn_suffix))-1])) {
                    o.syn_suffix[slen] = 'b'; o.syn_suffix[slen+1] = '\0';
                }
#else
                errsend(NONFATAL,"configure with --enable-syndata, to use option '-x'");
#endif
                break;
            case 'o':
                o.preserve = 1; // preserve ownership, during copies.
                break;
            case 'n':
                //different
                o.different = 1;  // falls through ... on purpose?
            case 'r':
                o.recurse = 1;
                break;
            case 'l':
                o.logging = 1;
                break;
            case 'P':
                o.parallel_dest = 1;
                break;
            case 'M':
                o.meta_data_only = 0;
                break;
            case 'v':
                // each '-v' increases verbosity, as follows
                //  = 0  minimal output enough to see that something is happening
                //  = 1  file-by-file chunk-by-chunk output
                //  = 2  also show the INFO messages from stat, etc
                o.verbose += 1;
                break;
            case 'g':
                // each '-g' increases diagnostics level, as follows
                //  = 1  means make a runtime infinite loop, for gdb
                //  = 2  means show S3 client/server interaction
                o.debug += 1;
                break;
            case 'e':
                strncpy(o.exclude, optarg, PATHSIZE_PLUS);
                o.exclude[PATHSIZE_PLUS-1] = '\0';
                break;
            case 'h':
                //Help -- incoming!
                usage();
                run=0;
                break;
            case '?':
                return -1;
            default:
                break;
            }
        }

        // Wait for someone to attach gdb, before proceeding.
        // Then you could do something like this:
        //   'ps -elf | grep pftool | egrep -v '(mpirun|grep)'
        //
        // With the minimal number of MPI tasks (4), that might look like this:
        //   4 S user 17435 17434  0 80 0 - 57505 hrtime 13:16 pts/2 00:00:00 pftool ...
        //   4 R user 17436 17434 75 80 0 - 57505 -      13:16 pts/2 00:00:05 pftool ...
        //   4 R user 17437 17434 75 80 0 - 57505 -      13:16 pts/2 00:00:05 pftool ...
        //   4 R user 17438 17434 75 80 0 - 57505 -      13:16 pts/2 00:00:05 pftool ...
        //
        // Thus, 17435 is rank 0 (manager), and 17438 is rank 3 (the first worker task).
        // Then,
        //   $ gdb pftool 17438     [capture the worker task we want to debug]
        //   (gdb) ^Z               [save for later]
        //   $ gdb pftool 17435     [capture the manager task that is looping]
        //   (gdb) fin              [exit __nanosleep_nocancel()]
        //   (gdb) fin              [exit __sleep()]
        //   (gdb) set var gdb=1    [allow the loop below to exit]
        //   (gdb) q                [quit debugging the manager rank]
        //   $ fg                   [resume debugging rank 3, set breaks, continue]
        //
        if (o.debug == 1) {
            volatile int gdb = 0; // don't optimize me, bro!
            while (!gdb) {
                fprintf(stderr, "spinning waiting for gdb attach\n");
                sleep(5);
            }
        }
        if (dest_path[0] != '\0' && (o.work_type == LSWORK)) {
            fprintf(stderr, "Invalid option set, do not  use option '-c' when listing files\n");
            return -1;
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // assure the minimal number of ranks exist
    if (nproc <= START_PROC) {
        fprintf(stderr, "Requires at least %d ranks\n", START_PROC +1);
        return -1;
    }

    //broadcast all the options
    MPI_Bcast(&o.verbose, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.debug, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.recurse, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.logging, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.dest_fstype, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.different, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.parallel_dest, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.work_type, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.meta_data_only, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.blocksize, 1, MPI_DOUBLE, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.chunk_at, 1, MPI_DOUBLE, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.chunksize, 1, MPI_DOUBLE, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.preserve, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.use_file_list, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(o.jid, 128, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(o.exclude, PATHSIZE_PLUS, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);

#ifdef GEN_SYNDATA
    MPI_Bcast(o.syn_pattern, 128, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(o.syn_suffix, SYN_SUFFIX_MAX, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.syn_size, 1, MPI_DOUBLE, MANAGER_PROC, MPI_COMM_WORLD);
#endif

    // Path factory might want to use some of these fields.
    // TBD: Maybe we also want the src files processed via enqueue_path(), below.
    // 
    PathFactory::initialize(&o, rank, nproc, src_path, dest_path);

#ifdef MARFS
    if (o.debug == 2) {
        aws_set_debug(1);
    }
#endif

    //Modifies the path based on recursion/wildcards
    //wildcard
    if (rank == MANAGER_PROC) {
        if ((optind < argc) && (o.use_file_list)) { // only one of them is enqueued, below
            printf("Provided sources via '-i' and on the command-line\n");
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        if(!o.use_file_list && 0 == strnlen(src_path, PATHSIZE_PLUS)) {
            printf("No souce was provided\n");
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        if((COMPAREWORK == o.work_type || COPYWORK == o.work_type) &&
                0 == strnlen(dest_path, PATHSIZE_PLUS)) {
            printf("No destination was provided\n");
            MPI_Abort(MPI_COMM_WORLD, -1);
        }

        //process remaining optind for * and multiple src files
        // stick them on the input_queue
        if (optind < argc) {
            enqueue_path(&input_queue_head, &input_queue_tail, src_path, &input_queue_count);
            for (i = optind; i < argc; ++i) {
                enqueue_path(&input_queue_head, &input_queue_tail, argv[i], &input_queue_count);
            }
        }
        else if (o.use_file_list) {
            //we were provided a file list
            //
            // NOTE: We'll just assume the file-list is stored on a POSIX
            //       filesys, so we don't have to add fgets() methods to all
            //       the PATH subclasses.
            FILE *fp;
            char list_path[PATHSIZE_PLUS];
            fp = fopen(o.file_list, "r");
            while (fgets(list_path, PATHSIZE_PLUS, fp) != NULL) {
                size_t path_len = strlen(list_path);
                if (list_path[path_len -1] == '\n') {
                    list_path[path_len -1] = '\0';
                }
                enqueue_path(&input_queue_head, &input_queue_tail, list_path, &input_queue_count);
            }
            fclose(fp);
        } else
            enqueue_path(&input_queue_head, &input_queue_tail, src_path, &input_queue_count);
        
        // run realpath on output dir
        char buf[PATHSIZE_PLUS];
        strcpy(buf, dest_path);
        do {
            strcpy(dest_path, buf);
            PathPtr p_dest(PathFactory::create(dest_path));
            if(NULL == p_dest->realpath(buf) && ENOENT != errno) {
                fprintf(stderr, "Failed to realpath dest_path: %s\n", dest_path);
                MPI_Abort(MPI_COMM_WORLD, -1);
            }
        } while(0 != strcmp(dest_path, buf));

        if (input_queue_head != input_queue_tail && (o.work_type == COPYWORK || o.work_type == COMPAREWORK)) {
                        PathPtr p_dest(PathFactory::create(dest_path));
            if (!p_dest->exists() || !p_dest->is_dir()) {
                printf("Multiple inputs and target '%s' is not a directory\n", dest_path);
                MPI_Abort(MPI_COMM_WORLD, -1);
            }
        }

        // loop though the input queue and make sure it does not match the dest_path
        // also check for anything that should be excluded
        path_list *prev = NULL;
        path_list *head = input_queue_head;
        while(head != NULL) {
            // realpath the src
            char buf[PATHSIZE_PLUS];
            strcpy(buf, head->data.path);
            do {
                strcpy(head->data.path, buf);
                PathPtr p_src(PathFactory::create(head->data.path));
                if(NULL == p_src->realpath(buf)) {
                    fprintf(stderr, "Failed to realpath src: %s\n", head->data.path);
                    MPI_Abort(MPI_COMM_WORLD, -1);
                }
            } while(0 != strcmp(head->data.path, buf));

            // checking dest_path against src
            if(o.work_type == COPYWORK) {
                if(0 == strcmp(dest_path, head->data.path)) {
                    printf("The file \"%s\" is both a source and destination\n", dest_path);
                    MPI_Abort(MPI_COMM_WORLD, -1);
                }
            }

            // check for exclusions
            if(0 == fnmatch(o.exclude, head->data.path, 0)) {
                path_list *oldHead;
                if (o.verbose >= 1) {
                    printf("Excluding: %s\n", head->data.path);
                }
                if(head == input_queue_head) {
                    input_queue_head = head->next;
                }
                if(head == input_queue_tail) {
                    input_queue_tail = prev;
                }
                if(NULL != prev) {
                    prev->next = head->next;
                }
                oldHead = head;
                head = head->next;
                free(oldHead);
                input_queue_count--;
            } else {
                prev = head;
                head = head->next;
            }
        }

        if(NULL == input_queue_head) {
            printf("No source was provided/all was excluded so no work will be done\n");
            run=0;
        }
    }

    // tell the wokers weather or not we are going to run
    MPI_Bcast(&run, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);

    // take on the role appropriate to our rank.
    if (run) {
        if (rank == MANAGER_PROC) {
            ret_val = manager(rank, o, nproc, input_queue_head, input_queue_tail, input_queue_count, dest_path);
        }
        else {
            worker(rank, o);
        }
    }

    MPI_Finalize();
    return ret_val;
}


// reduce a large number to e.g. "3.54 G"
void human_readable(char* buf, size_t buf_size, size_t value) {
    const char*    unit_name[] = { "", "k", "M", "G", "T", "P", NULL };
    unsigned unit = 0;
    float    remain = float(value);
    while ((remain > 1024) && (unit_name[unit +1])) {
        remain /= 1024;
        ++unit;
    }

    if (unit)
        sprintf(buf, "%3.1f %s", remain, unit_name[unit]);
    else
        sprintf(buf, "%ld ", value);
}

// taking care to avoid losing significant precision ...
float diff_time(struct timeval* later, struct timeval* earlier) {
    static const float MILLION   = 1000000.f;
    float n = later->tv_sec - earlier->tv_sec;
    n += (later->tv_usec / MILLION);
    n -= (earlier->tv_usec / MILLION);
    return n;
}

int manager(int             rank,
             struct options& o,
             int             nproc,
             path_list*      input_queue_head,
             path_list*      input_queue_tail,
             int             input_queue_count,
             const char*     dest_path) {

    MPI_Status  status;
    int         message_ready = 0;
    int         probecount = 0;
    int         prc;
    int         type_cmd;
    int         work_rank;
    int         sending_rank;
    int         i;
    struct worker_proc_status* proc_status;
    int         readdir_rank_count = 0;
    int         free_worker_count = 0; // only counts ranks >= START_PROC

    struct timeval in;
    struct timeval out;

    int         non_fatal = 0;

    int         examined_file_count = 0;
    int         examined_dir_count = 0;
    size_t      examined_byte_count = 0;
    size_t      finished_byte_count = 0;

    char        message[MESSAGESIZE];
    char        errmsg[MESSAGESIZE];
    char        base_path[PATHSIZE_PLUS];
    char        temp_path[PATHSIZE_PLUS];

    struct stat st;

    path_item   beginning_node = {0};
    path_item   dest_node;
    path_list*  iter = NULL;
    int         num_copied_files = 0;
    size_t      num_copied_bytes = 0;
    size_t      num_copied_bytes_prev = 0; // captured at previous timer

    work_buf_list* stat_buf_list         = NULL;
    work_buf_list* stat_buf_list_tail    = NULL;
    int            stat_buf_list_size    = 0;

    work_buf_list* process_buf_list      = NULL;
    work_buf_list* process_buf_list_tail = NULL;
    int            process_buf_list_size = 0;

    work_buf_list* dir_buf_list          = NULL;
    work_buf_list* dir_buf_list_tail     = NULL;
    int            dir_buf_list_size     = 0;

    int         mpi_ret_code;
    int         rc;
    int         start = 1;

    // for the "low-verbosity" output, we just periodically print
    // cumulative stats.  We create a non-interrupting timer which we just
    // poll.  However, that will give us not-very-perfect intervals, so we
    // also mark the TOD each time we detect the expiration.  That lets us
    // compute incremental BW more accurately.
    static const size_t  output_timeout = 10; // secs per expiration
    timer_t              timer;
    struct sigevent      event;
    struct itimerspec    itspec_new;
    struct itimerspec    itspec_cur;
    int                  timer_count = 0; // timer expirations
    struct timeval       now;   // time-of-day when the timer expired
    struct timeval       prev;  // previous expiration

    if (! o.verbose) {
        // create timer
        event.sigev_notify = SIGEV_NONE; // we'll poll for timeout
        if (timer_create(CLOCK_MONOTONIC, &event, &timer)) {
            errsend_fmt(FATAL, "failed to initialize timer '%s'\n", strerror(errno));
        }

        // initialize to expire after <output_timeout> sec
        itspec_new.it_value.tv_sec     = output_timeout;
        itspec_new.it_value.tv_nsec    = 0;
        itspec_new.it_interval.tv_sec  = 0;
        itspec_new.it_interval.tv_nsec = 0;

        if (timer_settime(timer, 0, &itspec_new, &itspec_cur)) {
            errsend_fmt(FATAL, "failed to set timer '%s'\n", strerror(errno));
        }
        // capture time-of-day, for accurate BW
        gettimeofday(&prev, NULL);
    }

    //path stuff
    int wildcard = 0;
    if (input_queue_count > 1) {
        wildcard = 1;
    }

    //make directories if it's a copy job
    int makedir = 0;
    if (o.work_type == COPYWORK) {
        makedir = 1;
    }

    //setup paths
    strncpy(beginning_node.path, input_queue_head->data.path, PATHSIZE_PLUS);
    get_base_path(base_path, &beginning_node, wildcard);
    if (o.work_type != LSWORK) {

        //need to stat_item sooner, we're doing a mkdir we shouldn't be doing, here.
        rc = stat_item(&beginning_node, o);
        get_dest_path(&dest_node, dest_path, &beginning_node, makedir, input_queue_count, o);
        ////            rc = stat_item(&dest_node, o); // now done in get_dest_path, via Factory

        // setup destination directory, if needed. Make sure -R has been specified!
        if (S_ISDIR(beginning_node.st.st_mode) && makedir == 1 && o.recurse){
            // NOTE: If we errsend anything here, we'll deadlock on the
            //       other procs that are waiting at the Bcast(),
            //       below.  That's because there's a problem with the
            //       way OUTPUT_PROC is used.  Either the errsend()
            //       functions should send asynchronously, or
            //       OUTPUT_PROC should run a special process (other
            //       than worker()), so that it does nothing but
            //       synchronous recvs of the diagnostic messages
            //       OUTCMD and LOGCMD.

            PathPtr p(PathFactory::create_shallow(&dest_node));

            // we need to use the permissions of the source filtering out other mode things
            p->mkdir(beginning_node.st.st_mode & (S_ISUID|S_ISGID|S_IRWXU|S_IRWXG|S_IRWXO));

            // TBD: Remove this.  This is just for now, because most of
            //       pftool still just looks at naked stat structs,
            //       inside Path objects.  Ours hasn't been initialized
            //       yet.  If you ask for any stat-related info from
            //       the Path object, it would do a stat before
            //       answering.  But if you just go look at the raw
            //       struct, e.g. with S_ISDIR(st.st_mode), you'll be
            //       looking at all zeros, and you'll think it isn't a
            //       directory.
            p->stat();
        }

        mpi_ret_code = MPI_Bcast(&dest_node, sizeof(path_item), MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
        if (mpi_ret_code < 0) {
            errsend(FATAL, "Failed to Bcast dest_path");
        }
    }

    mpi_ret_code = MPI_Bcast(base_path, PATHSIZE_PLUS, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
    if (mpi_ret_code < 0) {
        errsend(FATAL, "Failed to Bcast base_path");
    }

    // Make sure there are no multiple roots for a recursive operation
    // (because we assume we can use base_path to generate all destination paths?)
    // (because multiple roots imply recursive descent will iterate forever?)
    iter = input_queue_head;
    if ((o.recurse == 1)
        && strncmp(base_path, ".", PATHSIZE_PLUS)
        && (o.work_type != LSWORK)) {
        char iter_base_path[PATHSIZE_PLUS];
        while (iter != NULL) {
            get_base_path(iter_base_path, &(iter->data), wildcard);
            if (strncmp(iter_base_path, base_path, PATHSIZE_PLUS) != 0) {
                errsend(FATAL, "All sources for a recursive operation must be contained within the same directory.");
            }
            iter = iter->next;
        }
    }

    if(o.work_type != LSWORK) {
        //quick check that source is not nested
        char* copy = strdup(dest_path);
        strncpy(temp_path, dirname(copy), PATHSIZE_PLUS);
        free(copy);

        PathPtr p_dir(PathFactory::create((char*)temp_path));
        if (! p_dir->exists()) {
            fprintf(stderr, "manager: failed to create temp_path %s\n", temp_path);
            char err_cause[MESSAGESIZE];
            strerror_r(errno, err_cause, MESSAGESIZE);
            snprintf(errmsg, MESSAGESIZE, "parent doesn't exist: %s: %s", dest_path, err_cause);
            errsend(FATAL, errmsg);
        }

        // check to make sure that if the source is a directory, then -R was specified. If not, error out.
        if (S_ISDIR(beginning_node.st.st_mode) && !o.recurse)
            errsend_fmt(NONFATAL,"%s is a directory, but no recursive operation specified\n",beginning_node.path);
    }

    //pack our list into a buffer:
    pack_list(input_queue_head, input_queue_count, &dir_buf_list, &dir_buf_list_tail, &dir_buf_list_size);
    delete_queue_path(&input_queue_head, &input_queue_count);

    //allocate a vector to hold proc status for every proc
    proc_status = (struct worker_proc_status*)malloc(nproc * sizeof(struct worker_proc_status));

    //initialize proc_status
    for (i = 0; i < nproc; i++) {
        proc_status[i].inuse = 0;
        proc_status[i].readdir = 0;
    }
    free_worker_count = nproc - START_PROC;

    sprintf(message, "INFO  HEADER   ========================  %s  ============================\n", o.jid);
    write_output(message, 1);
    sprintf(message, "INFO  HEADER   Starting Path: %s\n", beginning_node.path);
    write_output(message, 1);

    {   
        PathPtr p_src(PathFactory::create(beginning_node.path));
        sprintf(message, "INFO  HEADER   Source-type: %s\n", p_src->class_name().get());
        write_output(message, 1);
        PathPtr p_dest(PathFactory::create(dest_path));
        sprintf(message, "INFO  HEADER   Dest-type:   %s\n", p_dest->class_name().get());
        write_output(message, 1);
    }

    //starttime
    gettimeofday(&in, NULL);

    // process responses from workers
    while (1) {
        //poll for message
        while ( message_ready == 0) {

            // check for availability of message (without reading it)
            prc = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD,
                             &message_ready, &status);
            if (prc != MPI_SUCCESS) {
                errsend(FATAL, "MPI_Iprobe failed\n");
                message_ready = -1;
            }
            else
                probecount++;


            if  (probecount % 3000 == 0) {
                PRINT_POLL_DEBUG("Rank %d: Waiting for a message\n", rank);
                PRINT_POLL_DEBUG("process_buf_list_size = %d\n", process_buf_list_size);
                PRINT_POLL_DEBUG("stat_buf_list_size = %d\n", stat_buf_list_size);
                PRINT_POLL_DEBUG("dir_buf_list_size = %d\n", dir_buf_list_size);

                // maybe break out of the probe loop, to provide timely output
                if (! o.verbose) {

                    if (timer_gettime(timer, &itspec_cur)) {
                        errsend_fmt(FATAL, "failed to get timer '%s'\n", strerror(errno));
                    }
                    if ((itspec_cur.it_value.tv_sec  == 0) &&
                        (itspec_cur.it_value.tv_nsec == 0)) {
                        break;
                    }
                }
            }



            // Always try to dish out work, before handling messages
            // Otherwise, we can be preoccupied with CHNKCMD msgs, for a big copy
            // NOTE: We're assuming the #ifdef TAPE is obsolete
            if (free_worker_count && process_buf_list_size) {
                for (i = 0; i < nproc; i++) {
                    PRINT_PROC_DEBUG("Rank %d, Status %d\n", i, proc_status.inuse[i]);
                }
                PRINT_PROC_DEBUG("=============\n");
                if (o.work_type == COPYWORK) {
                    for (i = 0; i < 3; i ++) {
                        work_rank = get_free_rank(proc_status, START_PROC, nproc - 1);
                        if (work_rank >= 0 && process_buf_list_size > 0) {
                            proc_status[work_rank].inuse = 1;
                            free_worker_count -= 1;
                            send_worker_copy_path(work_rank, &process_buf_list, &process_buf_list_tail, &process_buf_list_size);
                        }
                        else
                            break;
                    }
                }
                else if (o.work_type == COMPAREWORK) {
                    for (i = 0; i < 3; i ++) {
                        work_rank = get_free_rank(proc_status, START_PROC, nproc - 1);
                        if (work_rank >= 0 && process_buf_list_size > 0) {
                            proc_status[work_rank].inuse = 1;
                            free_worker_count -= 1;
                            send_worker_compare_path(work_rank, &process_buf_list, &process_buf_list_tail, &process_buf_list_size);
                        }
                        else
                            break;
                    }
                }
                else {
                    //delete the queue here
                    delete_buf_list(&process_buf_list, &process_buf_list_tail, &process_buf_list_size);
                }

            }

            if (dir_buf_list_size
                && ((-1 == o.max_readdir_ranks) || (readdir_rank_count < o.max_readdir_ranks))) {
                work_rank = get_free_rank(proc_status, START_PROC, nproc - 1);
                if (work_rank >= 0) {
                    if (((start == 1 || o.recurse) && dir_buf_list_size)) {
                        proc_status[work_rank].inuse   = 1;
                        free_worker_count  -= 1;
                        proc_status[work_rank].readdir = 1;
                        readdir_rank_count += 1;
                        send_worker_readdir(work_rank, &dir_buf_list, &dir_buf_list_tail, &dir_buf_list_size);
                        start = 0;
                    }
                    else if (!o.recurse) {
                        delete_buf_list(&dir_buf_list, &dir_buf_list_tail, &dir_buf_list_size);
                    }
                }
            }

            //are we finished?
            if (process_buf_list_size == 0
                && stat_buf_list_size == 0
                && dir_buf_list_size == 0
                && processing_complete(proc_status, free_worker_count, nproc)) {

                break;
            }

            if (! message_ready)
                usleep(1);
        }

        // got a message, or nothing left to do
        if (process_buf_list_size == 0
            && stat_buf_list_size == 0
            && dir_buf_list_size == 0
            && processing_complete(proc_status, free_worker_count, nproc)) {

            break;
        }

        if (message_ready) {

            // got a message, get message type
            if (MPI_Recv(&type_cmd, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status)
                != MPI_SUCCESS) {
                errsend(FATAL, "Failed to receive type_cmd\n");
            }

            sending_rank = status.MPI_SOURCE;
            PRINT_MPI_DEBUG("rank %d: manager() Receiving the command %s from rank %d\n",
                            rank, cmd2str((OpCode)type_cmd), sending_rank);
            //do operations based on the message
            switch(type_cmd) {
            case WORKDONECMD:
                //worker finished their tasks
                manager_workdone(rank, sending_rank, proc_status, &free_worker_count, &readdir_rank_count);
                break;
            case NONFATALINCCMD:
                //non fatal errsend encountered
                non_fatal++;
                break;
            case CHUNKBUSYCMD:
                proc_status[ACCUM_PROC].inuse = 1;
                // free_worker_count -= 1; // nope.  This is only for rank >= START_PROC
                break;
            case COPYSTATSCMD:
                manager_add_copy_stats(rank, sending_rank, &num_copied_files, &num_copied_bytes);
                break;
            case EXAMINEDSTATSCMD:
                manager_add_examined_stats(rank, sending_rank, &examined_file_count, &examined_byte_count, &examined_dir_count, &finished_byte_count);
                break;
            case PROCESSCMD:
                manager_add_buffs(rank, sending_rank, &process_buf_list, &process_buf_list_tail, &process_buf_list_size);
                break;
            case DIRCMD:
                manager_add_buffs(rank, sending_rank, &dir_buf_list, &dir_buf_list_tail, &dir_buf_list_size);
                break;
            case INPUTCMD:
                manager_add_buffs(rank, sending_rank, &stat_buf_list, &stat_buf_list_tail, &stat_buf_list_size);
                break;
            case QUEUESIZECMD:
                send_worker_queue_count(sending_rank, stat_buf_list_size);
                break;
            default:
                break;
            }
        }

        // for the "low-verbosity" output, we just periodically report
        // cumulative stats.  process_stat_buffer() only counts entire
        // files as "files examined", but copy_file() sees each chunk as a
        // "file".  Therefore, it would be confusing to print examined
        // files as a way to suggest how many files are still to be moved.
        // However, process_stat_buffer() does give us examined bytes which
        // is the total size of data to be moved, so that shows what is
        // left to be done.
        if (! o.verbose) {

            if (timer_gettime(timer, &itspec_cur)) {
                errsend_fmt(FATAL, "failed to set timer '%s'\n", strerror(errno));
            }
            if ((itspec_cur.it_value.tv_sec  == 0) &&
                (itspec_cur.it_value.tv_nsec == 0)) {

                // timer expired.  print cumulative stats
                ++ timer_count; // could be used to print a header

                // compute true elapsed time
                gettimeofday(&now, NULL);
                float interval_elapsed = diff_time(&now, &prev);
                float total_elapsed    = diff_time(&now, &in);
                // put numbers into a human-readable format
                static const size_t BUF_SIZE = 1024;
                char files    [BUF_SIZE];
                // char files_ex [BUF_SIZE];
                char bytes    [BUF_SIZE];
                char bytes_tbd[BUF_SIZE]; // total TBD, including <bytes>
                char bw       [BUF_SIZE]; // for just this interval
                char bw_avg   [BUF_SIZE];

                // compute BW for this period, and avg over run
                size_t bytes0 = (num_copied_bytes - num_copied_bytes_prev);
                float  bw0    = bytes0 / interval_elapsed; // (float)output_timeout;
                float  bw_tot = num_copied_bytes / total_elapsed; // (float)(timer_count * output_timeout);

                // human-readable representations
                human_readable(files,     BUF_SIZE, num_copied_files);
                // human_readable(files_ex,  BUF_SIZE, examined_file_count);
                human_readable(bytes,     BUF_SIZE, num_copied_bytes + finished_byte_count);
                human_readable(bytes_tbd, BUF_SIZE, examined_byte_count); // - num_copied_bytes);
                human_readable(bw,        BUF_SIZE, bw0); // this period
                human_readable(bw_avg,    BUF_SIZE, bw_tot);

                if (o.logging) {
                    // syslog includes the interval BW
                    sprintf(message,
                            "INFO ACCUM  files/chunks: %4s    "
                            "data: %8sB / %8sB    "
                            "BW: (interval: %8sB/s    overall: %8sB/s)    "
                            "errs: %d\n",
                            files, // files_ex,
                            bytes, bytes_tbd,
                            bw, bw_avg,
                            non_fatal);
                    write_output(message, 2); // syslog-only
                }
                sprintf(message,
                        "INFO ACCUM  files/chunks: %4s    "
                        "data: %8sB / %8sB    "
                        "avg BW: %8sB/s    "
                        "errs: %d\n",
                        files, // files_ex,
                        bytes, bytes_tbd,
                        bw_avg, // no incremental
                        non_fatal);
                write_output(message, 0); // stdout-only


                // save current byte-count, so we can see incremental changes
                num_copied_bytes_prev = num_copied_bytes; // measure BW per-timer
                // save current TOD, for accurate interval BW
                prev = now;
                // set timer for next interval
                if (timer_settime(timer, 0, &itspec_new, &itspec_cur)) {
                    errsend_fmt(FATAL, "failed to set timer '%s'\n", strerror(errno));
                }
            }
        }
        message_ready = 0;
    }

    gettimeofday(&out, NULL);
    int elapsed_time = out.tv_sec - in.tv_sec;

    //Manager is done, cleaning have the other ranks exit
    //make sure there's no pending output
    sprintf(message, "INFO  FOOTER   ========================   NONFATAL ERRORS = %d   ================================\n", non_fatal);
    write_output(message, 1);
    sprintf(message, "INFO  FOOTER   =================================================================================\n");
    write_output(message, 1);
    sprintf(message, "INFO  FOOTER   Total Files/Links Examined: %d\n", examined_file_count);
    write_output(message, 1);
    if (o.work_type == LSWORK) {
        sprintf(message, "INFO  FOOTER   Total Bytes Examined: %zd\n", examined_byte_count);
        write_output(message, 1);
    }
    sprintf(message, "INFO  FOOTER   Total Dirs Examined: %d\n", examined_dir_count);
    write_output(message, 1);

    if (o.work_type == COPYWORK) {
        sprintf(message, "INFO  FOOTER   Total Buffers Written: %d\n", num_copied_files);
        write_output(message, 1);
        sprintf(message, "INFO  FOOTER   Total Bytes Copied: %zd\n", num_copied_bytes);
        write_output(message, 1);
        if ((num_copied_bytes/(1024*1024)) > 0 ) {
            sprintf(message, "INFO  FOOTER   Total Megabytes Copied: %zd\n", (num_copied_bytes/(1024*1024)));
            write_output(message, 1);
        }
        if((num_copied_bytes/(1024*1024)) > 0 ) {
            sprintf(message, "INFO  FOOTER   Data Rate: %zd MB/second\n", (num_copied_bytes/(1024*1024))/(elapsed_time+1));
            write_output(message, 1);
        }
    }
    else if (o.work_type == COMPAREWORK) {
        sprintf(message, "INFO  FOOTER   Total Files Compared: %d\n", num_copied_files);
        write_output(message, 1);
        if (o.meta_data_only == 0) {
            sprintf(message, "INFO  FOOTER   Total Bytes Compared: %zd\n", num_copied_bytes);
            write_output(message, 1);
        }
    }

    if (elapsed_time == 1) {
        sprintf(message, "INFO  FOOTER   Elapsed Time: %d second\n", elapsed_time);
    }
    else {
        sprintf(message, "INFO  FOOTER   Elapsed Time: %d seconds\n", elapsed_time);
    }

    write_output(message, 1);
    for(i = 1; i < nproc; i++) {
        send_worker_exit(i);
    }

    free(proc_status);

    // return nonzero for any errors
    if (0 != non_fatal) {
        return 1;
    }
    return 0;
}

// recv <path_count>, then a block of packed data.  Unpack to individual
// path_items, pushing onto tail of queue
int manager_add_paths(int rank, int sending_rank, path_list **queue_head, path_list **queue_tail, int *queue_count) {
    MPI_Status  status;
    int         path_count;
    path_list*  work_node = (path_list*)malloc(sizeof(path_list));
    char        path[PATHSIZE_PLUS];
    char *      workbuf;
    int         worksize;
    int         position;
    int         i;

    if (! work_node) {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for work_node\n", sizeof(path_list));
    }

    //gather the # of files
    PRINT_MPI_DEBUG("rank %d: manager_add_paths() Receiving path_count from rank %d\n", rank, sending_rank);
    if (MPI_Recv(&path_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive path_count\n");
    }
    worksize =  path_count * sizeof(path_list);
    workbuf  = (char *) malloc(worksize * sizeof(char));
    if (! workbuf) {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for workbuf\n", sizeof(worksize));
    }

    //gather the path to stat
    PRINT_MPI_DEBUG("rank %d: manager_add_paths() Receiving worksize from rank %d\n", rank, sending_rank);
    if (MPI_Recv(workbuf, worksize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }

    position = 0;
    for (i = 0; i < path_count; i++) {
        PRINT_MPI_DEBUG("rank %d: manager_add_paths() Unpacking the work_node from rank %d\n", rank, sending_rank);
        MPI_Unpack(workbuf, worksize, &position, &work_node->data, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);
        // // The following appears to be useless ...
        // strncpy(path, work_node->data.path, PATHSIZE_PLUS);
        enqueue_node(queue_head, queue_tail, work_node, queue_count);
    }

    free(work_node);
    free(workbuf);
    return path_count;
}

// recv <path_count>, then a block of packed data.  Push block onto a work_buf_list
void manager_add_buffs(int rank, int sending_rank, work_buf_list **workbuflist, work_buf_list **workbuflisttail, int *workbufsize) {
    MPI_Status  status;
    int         path_count;
    char*       workbuf;
    int         worksize;

    //gather the # of files
    PRINT_MPI_DEBUG("rank %d: manager_add_buffs() Receiving path_count from rank %d\n", rank, sending_rank);
    if (MPI_Recv(&path_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive path_count\n");
    }
    worksize =  path_count * sizeof(path_list);
    workbuf = (char *) malloc(worksize * sizeof(char));
    if (! workbuf) {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for workbuf\n", sizeof(workbuf));
    }

    //gather the path to stat
    PRINT_MPI_DEBUG("rank %d: manager_add_buffs() Receiving buff from rank %d\n", rank, sending_rank);
    if (MPI_Recv(workbuf, worksize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }
    if (path_count > 0) {
        enqueue_buf_list(workbuflist, workbuflisttail, workbufsize, workbuf, path_count);
    }
}

void manager_add_copy_stats(int rank, int sending_rank, int *num_copied_files, size_t *num_copied_bytes) {
    MPI_Status status;
    int num_files;
    size_t num_bytes;
    //gather the # of copied files
    PRINT_MPI_DEBUG("rank %d: manager_add_copy_stats() Receiving num_copied_files from rank %d\n", rank, sending_rank);
    if (MPI_Recv(&num_files, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }
    //gather the # of copied byes
    PRINT_MPI_DEBUG("rank %d: manager_add_copy_stats() Receiving num_copied_bytes from rank %d\n", rank, sending_rank);
    if (MPI_Recv(&num_bytes, 1, MPI_DOUBLE, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }
    *num_copied_files += num_files;
    *num_copied_bytes += num_bytes;
}

void manager_add_examined_stats(int rank, int sending_rank, int *num_examined_files, size_t *num_examined_bytes, int *num_examined_dirs, size_t *num_finished_bytes) {
    MPI_Status status;
    int        num_files = 0;
    size_t     num_bytes = 0;
    int        num_dirs = 0;
    size_t     num_bytes_finished = 0;

    //gather the # of examined files
    PRINT_MPI_DEBUG("rank %d: manager_add_examined_stats() Receiving num_examined_files from rank %d\n", rank, sending_rank);
    if (MPI_Recv(&num_files, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }
    PRINT_MPI_DEBUG("rank %d: manager_add_examined_stats() Receiving num_examined_bytes from rank %d\n", rank, sending_rank);
    if (MPI_Recv(&num_bytes, 1, MPI_DOUBLE, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }
    PRINT_MPI_DEBUG("rank %d: manager_add_examined_stats() Receiving num_examined_dirs from rank %d\n", rank, sending_rank);
    if (MPI_Recv(&num_dirs, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }
    PRINT_MPI_DEBUG("rank %d: manager_add_examined_stats() Receiving num_finished_bytes from rank %d\n", rank, sending_rank);
    if (MPI_Recv(&num_bytes_finished, 1, MPI_DOUBLE, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }

    *num_examined_files += num_files;
    *num_examined_bytes += num_bytes;
    *num_examined_dirs += num_dirs;
    *num_finished_bytes += num_bytes_finished;
}


void manager_workdone(int rank, int sending_rank, struct worker_proc_status *proc_status, int* free_worker_count, int* readdir_rank_count) {
    if (proc_status[sending_rank].inuse) {
        proc_status[sending_rank].inuse = 0;
        if (sending_rank >= START_PROC)
            *free_worker_count += 1;
    }
    if (proc_status[sending_rank].readdir) {
        proc_status[sending_rank].readdir = 0;
        *readdir_rank_count -= 1;
    }
}

void worker(int rank, struct options& o) {
    MPI_Status status;
    int sending_rank;
    int all_done = 0;
    int makedir = 0;
    int message_ready = 0, probecount = 0;
    int prc;
    char*     output_buffer = (char*)NULL;
    int       type_cmd;
    int       mpi_ret_code;
    char      base_path[PATHSIZE_PLUS];
    path_item dest_node;

    //variables stored by the 'accumulator' proc
    HASHTBL*  chunk_hash;
    int       base_count = 100;
    int       hash_count = 0;
    int       output_count = 0;


    // OUTPUT_PROC could just sits out the Bcast of dest_node and base path
    // (if we used a communicator without him).  OUTPUT_PROC won't need
    // those, and waiting at the Bcast means anybody else who calls
    // errsend() before hitting the Bcast will deadlock everything.
    if (rank == OUTPUT_PROC) {
        const size_t obuf_size = MESSAGEBUFFER * MESSAGESIZE * sizeof(char);
        output_buffer = (char *) malloc(obuf_size);
        if (! output_buffer) {
            // // This would never work ...
            fprintf(stderr, "OUTPUT_PROC Failed to allocate %lu bytes "
                    "for output_buffer\n", obuf_size);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        memset(output_buffer, '\0', obuf_size);
    }
    if (o.work_type == COPYWORK) {
        makedir = 1;
    }
    if (o.work_type != LSWORK) {
        mpi_ret_code = MPI_Bcast(&dest_node, sizeof(path_item), MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
        if (mpi_ret_code < 0) {
            errsend(FATAL, "Failed to Receive Bcast dest_path");
        }
    }
    mpi_ret_code = MPI_Bcast(base_path, PATHSIZE_PLUS, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
    if (mpi_ret_code < 0) {
        errsend(FATAL, "Failed to Receive Bcast base_path");
    }
    get_stat_fs_info(base_path, &o.sourcefs);
    if (o.parallel_dest == 0 && o.work_type != LSWORK) {
        get_stat_fs_info(dest_node.path, &o.destfs);
        if (o.destfs >= PARALLEL_DESTFS) {
            o.parallel_dest = 1;
        }
    }
    

    // can't do this before the Bcast above, or we'll deadlock, because
    // output-proc won't yet be listening for work.
    if (rank == ACCUM_PROC) {
        if(!(chunk_hash=hashtbl_create(base_count, NULL))) {
            errsend(FATAL, "hashtbl_create() failed\n");
        }
    }

    //change this to get request first, process, then get work
    while ( all_done == 0) {
        //poll for message
        while ( message_ready == 0) {
            prc = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &message_ready, &status);
            if (prc != MPI_SUCCESS) {
                errsend(FATAL, "MPI_Iprobe failed\n");
            }
            else
                probecount++;

            if  (probecount % 3000 == 0) {
                PRINT_POLL_DEBUG("Rank %d: Waiting for a message\n", rank);
            }
            if (! message_ready)
                usleep(1);
        }

        //grab message type
        if (MPI_Recv(&type_cmd, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
            errsend(FATAL, "Failed to receive type_cmd\n");
        }
        sending_rank = status.MPI_SOURCE;
        PRINT_MPI_DEBUG("rank %d: worker() Receiving the type_cmd %s from rank %d\n", rank, cmd2str((OpCode)type_cmd), sending_rank);
        //do operations based on the message
        switch(type_cmd) {
            case BUFFEROUTCMD:
                worker_buffer_output(rank, sending_rank, output_buffer, &output_count, o);
                break;
            case OUTCMD:
                worker_output(rank, sending_rank, 0, output_buffer, &output_count, o);
                break;
            case LOGCMD:
                worker_output(rank, sending_rank, 1, output_buffer, &output_count, o);
                break;
            case LOGONLYCMD:
                worker_output(rank, sending_rank, 2, output_buffer, &output_count, o);
                break;
            case UPDCHUNKCMD:
                worker_update_chunk(rank, sending_rank, &chunk_hash, &hash_count, base_path, &dest_node, o);
                break;
            case DIRCMD:
                worker_readdir(rank, sending_rank, base_path, &dest_node, 0, makedir, o);
                break;
            case COPYCMD:
                worker_copylist(rank, sending_rank, base_path, &dest_node, o);
                break;
            case COMPARECMD:
                worker_comparelist(rank, sending_rank, base_path, &dest_node, o);
                break;
            case EXITCMD:
                all_done = 1;
                break;
            default:
                errsend(FATAL, "worker received unrecognized command\n");
                break;
        }
        message_ready = 0;
    }

    // cleanup
    if (rank == ACCUM_PROC) {
        hashtbl_destroy(chunk_hash);
    }
    if (rank == OUTPUT_PROC) {
        worker_flush_output(output_buffer, &output_count);
        free(output_buffer);
    }
}

/**
 * A worker task that updates a "database" of files that have been chunked during
 * a transfer. 
 *
 * This routine reads a hashtable containing stuctures that describe
 * the chunked file, and updates the appropriate structure when a chunk of a 
 * file has been transferred.
 *
 * @param rank       the MPI rank of the current process (usually 2 for
 *           this task)
 * @param sending_rank   the rank of the MPI process that sent the request to
 *           update the chunk.
 * @param chunk_hash a pointer to the hash table that contains the structures
 *           that describe the chunked files
 * @param hash_count the length of the hash table
 * @param base_path   used to generate the output path
 * @param dest_node  a potentially sparsely path_item that is used to generate
 *          the full output path. 
 * @param o      PFTOOL global/command options
 */
// TODO: Check void worker_update_chunk(int rank, int sending_rank, HASHTBL **chunk_hash, int *hash_count, const char *base_path, path_item dest_node, struct options o) {
void worker_update_chunk(int            rank,
                         int            sending_rank,
                         HASHTBL**      chunk_hash,
                         int*           hash_count,
                         const char*    base_path,
                         path_item*     dest_node,
                         struct options& o) {
    MPI_Status  status;
    int         path_count;
    path_item   work_node;
    path_item   out_node;
    char*       workbuf;
    int         worksize;
    int         position;
    HASHDATA*   hash_value;
    int         i;

    //gather the # of files
    if (MPI_Recv(&path_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive path_count\n");
    }
    PRINT_MPI_DEBUG("rank %d: worker_update_chunk() Receiving path_count from rank %d (path_count = %d)\n", rank, sending_rank,path_count);
    worksize =  path_count * sizeof(path_list);
    workbuf = (char *) malloc(worksize * sizeof(char));
    if (! workbuf) {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for workbuf\n", sizeof(workbuf));
    }

    //get the work nodes
    if (MPI_Recv(workbuf, worksize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }

    // process list of paths with completed chunks
    position = 0;
    for (i = 0; i < path_count; i++) {
        MPI_Unpack(workbuf, worksize, &position, &work_node, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);

        PRINT_MPI_DEBUG("rank %d: worker_update_chunk() Unpacking the work_node from rank %d (chunk %d of file %s)\n", rank, sending_rank, work_node.chkidx, work_node.path);

        // CTM is based off of destination file. Populate out_node
        get_output_path(&out_node, base_path, &work_node, dest_node, o);

        // let sub-classes do any per-chunk work they want to do
        //        PathPtr p_out(PathFactory::create_shallow(out_node));
        //        p_out->chunk_complete();
        Path::ChunkInfo chunk_info;
        chunk_info.index = work_node.chkidx;
        chunk_info.size  = work_node.chksz;

        size_t chunk_start = (chunk_info.index * chunk_info.size);
        size_t chunk_end   = chunk_start + chunk_info.size;
        if (chunk_end > work_node.st.st_size) {
            chunk_info.size = work_node.st.st_size - chunk_start;
        }

        // don't update chunk-info unless this is a COPY task.
        // (Only affects MarFS, currently)
        if (o.work_type == COPYWORK) {
            // just call the update per-chunk, instead of trying to accumulate updates
            Path::ChunkInfoVec vec;
            vec.push_back(chunk_info);
        
            PathPtr      p_out(PathFactory::create_shallow(&out_node));
            p_out->chunks_complete(vec);
        }

        out_node.chkidx = work_node.chkidx;                   // with necessary data from work_node.
        out_node.chksz = work_node.chksz;
        out_node.st.st_size = work_node.st.st_size;
        hash_value = hashtbl_get(*chunk_hash, out_node.path);             // get the value 
        if (hash_value == (HASHDATA *)NULL) {

            //resize the hashtable if needed
            if (*hash_count == (*chunk_hash)->size) {
                hashtbl_resize(*chunk_hash, *hash_count+100);
            }
            *hash_count += 1;

            if(hash_value = hashdata_create(out_node)) {
                hashtbl_insert(*chunk_hash, out_node.path, hash_value);
                hashdata_update(hash_value,out_node);                        // make sure the new structure has recorded this chunk!
            }
        }
        else {                                          // --- Structure for File needs to be updated
            hashdata_update(hash_value,out_node);                      // this will update the data in the table
            if(IO_DEBUG_ON) {
                char ctm_flags[2048];
                char *ctmstr = ctm_flags;
                int ctmlen = 2048;

                PRINT_IO_DEBUG("rank %d: worker_update_chunk() Updating CTM "
                               "(chunk %d of file %s)\n%s\n",
                               rank, out_node.chkidx, out_node.path,
                               tostringCTM((CTM *)hash_value, &ctmstr, &ctmlen));
            }
        }

        if (hash_value == (HASHDATA *)NULL) {                            // if no hash_value at this point, we have a problem!
            errsend(NONFATAL, "Do not have a hashed data structure for a chunked file!\n");
        }
        else if (hashdata_filedone(hash_value)) {                       // --- File is done transferring
            PRINT_IO_DEBUG("rank %d: worker_update_chunk() Last Chunk transferred. "
                           "CTM should be removed. (chunk %d of file %s)\n",
                           rank, out_node.chkidx, out_node.path);
            hash_value = hashtbl_remove(*chunk_hash, out_node.path);               // remove structure for File from hash table
            hashdata_destroy(&hash_value);                          // we are done with the data

            PathPtr p_work(PathFactory::create_shallow(&work_node));
            PathPtr p_out(PathFactory::create_shallow(&out_node));
            update_stats(p_work, p_out, o);
        }
    }

    free(workbuf);
    send_manager_work_done(rank);
}

// log == 0    output to stdout (ONLY)
// log == 1    output to syslog (AND stdout)
// log == 2    output to syslog (ONLY)
//
void worker_output(int rank, int sending_rank, int log, char *output_buffer, int *output_count, struct options& o) {
    //have a worker receive and print a single message
    MPI_Status status;
    char msg[MESSAGESIZE];
    char sysmsg[MESSAGESIZE + 50];

    static int needs_open = 1;
    if (needs_open) {
        sprintf(sysmsg, "pftool [%s] -- ", o.jid);
        openlog (sysmsg, (LOG_PID | LOG_CONS), LOG_USER);
        needs_open = 0;
    }

    //gather the message to print
    if (MPI_Recv(msg, MESSAGESIZE, MPI_CHAR, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive msg\n");
    }
    PRINT_MPI_DEBUG("rank %d: worker_output() Receiving the message from rank %d\n", rank, sending_rank);
    if (log && o.logging) {
        syslog (LOG_INFO, "%s", msg);
    }
    if (log < 2) {
        if (sending_rank == MANAGER_PROC){
            printf("%s", msg);
        }
        else{
            printf("RANK %3d: %s", sending_rank, msg);
        }
        fflush(stdout);
    }
}

void worker_buffer_output(int rank, int sending_rank, char *output_buffer, int *output_count, struct options& o) {
    //have a worker receive and print a single message
    MPI_Status status;
    int message_count;
    char msg[MESSAGESIZE];
    char *buffer;
    int buffersize;
    int position;
    int i;

    //gather the message_count
    PRINT_MPI_DEBUG("rank %d: worker_buffer_output() Receiving the message_count from %d\n", rank, sending_rank);
    if (MPI_Recv(&message_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive message_count\n");
    }
    buffersize = MESSAGESIZE*message_count;
    buffer = (char *) malloc(buffersize * sizeof(char));
    if (! buffer) {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for workbuf\n", buffersize);
    }

    //gather the path to stat
    PRINT_MPI_DEBUG("rank %d: worker_buffer_output() Receiving the buffer from %d\n", rank, sending_rank);
    if (MPI_Recv(buffer, buffersize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive buffer\n");
    }
    position = 0;
    for (i = 0; i < message_count; i++) {
        PRINT_MPI_DEBUG("rank %d: worker_buffer_output() Unpacking the message from %d\n", rank, sending_rank);
        MPI_Unpack(buffer, buffersize, &position, msg, MESSAGESIZE, MPI_CHAR, MPI_COMM_WORLD);
        printf("RANK %3d: %s", sending_rank, msg);
    }
    free(buffer);
    fflush(stdout);
}


void worker_flush_output(char *output_buffer, int *output_count) {
    if (*output_count > 0) {
        printf("%s", output_buffer);
        (*output_count) = 0;
        memset(output_buffer,'\0', sizeof(output_count));
    }
}


//When a worker is told to readdir, it comes here
void worker_readdir(int         rank,
        int         sending_rank,
        const char* base_path,
        path_item*  dest_node,
        int         start,
        int         makedir,
        struct options& o) {

    MPI_Status  status;
    char *      workbuf;
    int         worksize;
    int         position;
    int         read_count;
    char        path[PATHSIZE_PLUS];
    char        full_path[PATHSIZE_PLUS];
    char        errmsg[MESSAGESIZE];
    path_item   mkdir_node;
    path_item   work_node;
    path_item   workbuffer[STATBUFFER];
    int         buffer_count = 0;
    DIR*           dip;
    struct dirent* dit;
    start = 1;
    //filelist
    FILE *fp;
    int i, rc;

    // recv number of path_items being sent
    PRINT_MPI_DEBUG("rank %d: worker_readdir() Receiving the read_count %d\n", rank, sending_rank);
    if (MPI_Recv(&read_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive read_count\n");
    }

    worksize = read_count * sizeof(path_list);
    workbuf = (char *) malloc(worksize * sizeof(char));
    if (! workbuf) {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for workbuf\n", worksize);
    }

    //recv packed path_items
    PRINT_MPI_DEBUG("rank %d: worker_readdir() Receiving the workbuf %d\n", rank, sending_rank);
    if (MPI_Recv(workbuf, worksize, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive workbuf\n");
    }

    // unpack and process successive source-paths
    position = 0;
    for (i = 0; i < read_count; i++) {
        PRINT_MPI_DEBUG("rank %d: worker_readdir() Unpacking the work_node %d\n", rank, sending_rank);
        MPI_Unpack(workbuf, worksize, &position, &work_node, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);
        // <p_work> is an appropriately-selected Path subclass, which has
        // an _item member that points to <work_node>
        PRINT_MPI_DEBUG("rank %d: worker_readdir() PathFactory::cast(%d)\n", rank, (unsigned)work_node.ftype);
        PathPtr p_work = PathFactory::create_shallow(&work_node);
        
        if (work_node.start == 1) {
            if (! p_work->exists()) { // performs a stat()
                errsend_fmt(((o.work_type == LSWORK) ? NONFATAL : FATAL),
                        "Failed to stat path (1) %s\n", p_work->path());
                if (o.work_type == LSWORK)
                    return;
            }
            workbuffer[buffer_count] = work_node;
            buffer_count++;
        }
        else {
            // work_node is a source-directory.  Read file-names from it,
            // construct full source-side pathnames.  Eventually these go
            // to process_stat_buffer(), where they are converted to
            // destination-paths.
            if (! p_work->opendir()) {
                errsend_fmt(NONFATAL, "Failed to open (%s) dir %s [%s]\n", 
                            p_work->class_name().get(), p_work->path(),p_work->strerror());
            }

            if (makedir == 1) {
                get_output_path(&mkdir_node, base_path, &p_work->node(), dest_node, o);
                PathPtr p_dir(PathFactory::create_shallow(&mkdir_node));
                if (! p_dir->mkdir(p_work->node().st.st_mode & (S_ISUID|S_ISGID|S_IRWXU|S_IRWXG|S_IRWXO))
                    && (p_dir->get_errno() != EEXIST)) {
                    errsend_fmt(FATAL, "Failed to mkdir (%s) '%s'\n", 
                                p_dir->class_name().get(), p_dir->path());
                }
            }
            strncpy(path, p_work->path(), PATHSIZE_PLUS);

            // assure <path> ends with a single slash
            trim_trailing('/', path);
            size_t path_len  = strlen(path);
            path[path_len] = '/';
            path_len      += 1;
            path[path_len] = 0;

            // NOTE: dir-entry names will be directly appended to the tail of <path>
            char*  append_path = path + path_len; // ptr to end of directory-name
            size_t append_len  = PATHSIZE_PLUS - path_len;

            // Use readdir() to append each directory-entry directly onto
            // to the tail of <path>.  Path::readdir() returns false only
            // for errors.  EOF is signalled by returning with a
            // zero-length entry.
            bool   readdir_p;
            while (readdir_p = p_work->readdir(append_path, append_len)) {
                if (! *append_path) {
                    break;      // end of directory entries
                }
                if (strncmp(append_path, ".", PATHSIZE_PLUS) != 0 &&
                        strncmp(append_path, "..", PATHSIZE_PLUS) != 0) {
                    // check to see if we should skip it
                    if( 0 == fnmatch(o.exclude, path, 0) ) {
                        if (o.verbose >= 1) {
                            char message[MESSAGESIZE];
                            sprintf(message, "Excluding: %s\n", path);
                            write_output(message, 1);
                        }
                    } 
                    else {
                        // full-path is <path> + "/" + readdir()
                        PathPtr p_new = PathFactory::create(path);
                        if (! p_new->exists()) {
                            errsend_fmt(((o.work_type == LSWORK) ? NONFATAL : FATAL),
                                    "Failed to stat path (2) %s\n", p_new->path());
                            if (o.work_type == LSWORK)
                                return;
                        }

                        workbuffer[buffer_count] = p_new->node();
                        buffer_count++;
                        if (buffer_count != 0 && buffer_count % STATBUFFER == 0) {
                            process_stat_buffer(workbuffer, &buffer_count, base_path, dest_node, o, rank);
                        }
                    }
                }
            }

            // did the readdir() loop exit because of an error?
            if (! readdir_p) {
                errsend_fmt(NONFATAL, "readdir (entry %d) failed on %s (%s)\n",
                        buffer_count, work_node.path, p_work->strerror());
            }
            // done with 
            if (! p_work->closedir()) {
                errsend_fmt(NONFATAL, "Failed to close (%s) dir %s [%s]\n", 
                        p_work->class_name().get(), p_work->path(),p_work->strerror());
            }
        }
    }

    // process any remaining partially-filled workbuffer contents
    while(buffer_count != 0) {
        process_stat_buffer(workbuffer, &buffer_count, base_path, dest_node, o, rank);
    }

    free(workbuf);
    send_manager_work_done(rank);
}


// helper for process_stat_buffer avoids duplicated code
//
// This is called once only, before any copies are started from a given
// source-file.  This allows any one-time initializations that might be
// needed by the destination Path-subclass.
//
// For example, this gives MarFS a chance to initialize xattrs.  This
// allows us to choose the repo to use, based on the full input-file-size,
// rather than the size given to movers for individual chunks.
//
// This should not be called on files that already exist (e.g. N:1 files
// that already have received some writes and are being restarted).  This
// should always be called on files that are being written for the first
// time (e.g. never written, or previous version was unlinked).
// process_stat_buffer() attempts to assure these conditions, when calling.
//
// NOTE: This should be called before any file-copy ops (regbuffer) are
//     sent to the manager (for a given work-file).
//
// NOTE: pre_process initializes the destination-file as though it will be
//     written.  We only want that for COPYWORK.
//

int maybe_pre_process(int&         pre_process,
                      const struct options& o,
                      PathPtr&     p_out,
                      PathPtr&     p_work) {

    int ret;
    if (pre_process == 1 &&
        (o.work_type == COPYWORK))
    {
	//we are working with a either a size 0 file or a packable file
	//or a non chunkable non packable file
	//do not need to create temporary file
        if (!p_out->pre_process(p_work))
        	return -1;
    }
    else if (pre_process == 2 &&
        (o.work_type == COPYWORK))
    {
	//we are working with a chunkable file
	p_out->create_temporary_path(p_work->get_timestamp());
	if (!p_out->pre_process(p_work))
	{
		return -1;
	}
	else
	{
		//create CTM NOW
		//first restore to original output path
		p_out->restore_original_path();
		create_CTM(p_out->path(), p_work->path(), p_work->get_timestamp());
	}
    }

    return 0;
}

// This routine sometimes sets ftype = NONE.  In the FUSE_CHUNKER case, the
// routine later checks for ftype==NONE.  In other cases, these path_items
// that have been reset to NONE are being built into path_buffers and sent
// away.  What will happen is that whoever receives them (and constructs
// Path objects) will use stat_item() to rediscover an appropriate ftype
// for them.  It looks like what we're doing is destroying any existing
// destinations that match our out_node, and resetting out_node to an ftype
// that stat_item will re-consider.

// We've started using the new C++ classes to provide generic access to
// path_item structs, allowing support for S3, HDFS, etc, to be added
// without changing the code.  However, there is some code here that would
// require undue effort to add to the object-interface (for instance,
// different updates to path_item.length, depending on the value of ftype
// or dest_ftype).  It's easy to let these things continue to be done
// directly on the path_item, because the object (built via
// PathFactory::create_shallow) points at the same object.  No updates are
// done here which the object classes should care about (e.g. no changes to
// path-names, etc).  Therefore, the two can co-exist.  HOWEVER, if you add
// any code that does change "important" parts of the path_item
// (e.g. changing the path), then you should realize that the Path object
// will become out-of-sync until you do another Path::stat() call.  You can
// alter dest_ftype, and you can read ftype, but do not alter ftype and
// continue to use a Path object that was constructed with a
// shallow-pointer to that path_item.
//
// QUESTION: the C++ classes can't support setting path_item.ftype to NONE,
//     for a path_item that has any further use, unless it is going to be
//     passed through stat_item() again.  It appears to me that setting
//     path_item.ftype=NONE was used here for the sole purpose of
//     indicating that a file had been unlinked, and needed no further
//     treatment.  However, I actually did need to use that path_item again
//     later.  So, instead of setting ftype=NONE, I coined a new variable
//     ('out_unlinked') to indicate when the path_item had been unlinked.
//     Am I missing anything?  (For example, is someone ever going to look
//     at the path_item somewhere else, and do the wrong thing, because
//     it's ftype is not NONE?)
//
/**
 * This routine processes a buffer of path_items that represent files
 * that have been stat'ed, and are ready to put on the work list.
 *
 * @param *path_buffer   the buffer to process
 * @param *stat_count    the number of path_items (files) in
 *                         the given path_buffer
 * @param base_path      the base or parent directory of the
 *                         files being processed
 * @param dest_node      a path_item structure that is a template
 *                         for the destination of the transfer
 * @param o              the PFTOOL global options structure
 * @param rank           the process MPI rank of the (worker) process
 *                         doing the buffer processing
 */
void process_stat_buffer(path_item*      path_buffer,
                         int*            stat_count,
                         const char*     base_path,
                         path_item*      dest_node,
                         struct options& o,
                         int             rank) {

    //When a worker is told to stat, it comes here
    int         out_position;
    char*       writebuf;
    int         writesize;
    int         write_count = 0;
    int         num_examined_files = 0;
    size_t      num_examined_bytes = 0;
    size_t      num_finished_bytes = 0;
    int         num_examined_dirs = 0;
    char        errmsg[MESSAGESIZE];
    char        statrecord[MESSAGESIZE];
    path_item   out_node;
    int         out_unlinked = 0;
    int         process = 0;
    int         pre_process = 0;
    int         parallel_dest = 0;
    int         dest_exists = FALSE; // the destination already exists?
    struct tm   sttm;
    char        modebuf[15];
    char        timebuf[30];
    int         rc;
    int         i;
    char        timestamp[DATE_STRING_MAX];
    time_t      tp = time(NULL);
    epoch_to_str(timestamp, DATE_STRING_MAX, &tp);
    printf("Unique stamp %s\n", timestamp);

    //chunks
    //place_holder for current chunk_size
    size_t      chunk_size = 0;
    size_t      chunk_at = 0;
    size_t      num_bytes_seen = 0;

    // when chunking, we ship the list of chunks off as soon as they
    // represent more than <ship_off> bytes, in total.  For Marfs, that
    // means every single chunk is likely to be shipped off individually.
    // Maybe this should be bigger.
    size_t      ship_off = SHIPOFF;
    off_t       chunk_curr_offset = 0;
    int         idx = 0;
    //classification
    path_item   dirbuffer[DIRBUFFER];
    int         dir_buffer_count = 0;
    path_item   regbuffer[COPYBUFFER];
    int         reg_buffer_count = 0;

    writesize = MESSAGESIZE * MESSAGEBUFFER;
    writebuf = (char *) malloc(writesize * sizeof(char));
    if (! writebuf) {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for writebuf\n", writesize);
    }

    out_position = 0;
    for (i = 0; i < *stat_count; i++) {
        process = 0;
        pre_process = 0;
        path_item&  work_node = path_buffer[i]; // avoid a copy
 	memcpy(work_node.timestamp, timestamp, DATE_STRING_MAX); //first copy timestamp in work_node, if we have a temoprary file, it will be recopied from CTM
        work_node.start = 0;
        PathPtr p_work(PathFactory::create_shallow(&path_buffer[i]));
        PathPtr p_dest(PathFactory::create_shallow(dest_node));
        PathPtr p_out;

        PRINT_IO_DEBUG("rank %d: process_stat_buffer() processing entry %d: %s\n",
                       rank, i, work_node.path);
        // Are these items *identical* ? (e.g. same POSIX inode)
        // We will not have a dest in list so we will not check
        if ((o.work_type != LSWORK)
            && p_work->identical(p_dest)) {
            write_count--;
            continue;
        }

        //check if the work is a directory
        else if (p_work->is_dir()) {
            dirbuffer[dir_buffer_count] = p_work->node();  //// work_node;
            dir_buffer_count++;
            if (dir_buffer_count % DIRBUFFER == 0) {
                send_manager_dirs_buffer(dirbuffer, &dir_buffer_count);
            }
            num_examined_dirs++;
        }

        //it's not a directory
        else {
            //do this for all regular files AND fuse+symylinks
            parallel_dest = o.parallel_dest;
            get_output_path(&out_node, base_path, &work_node, dest_node, o);
            p_out = PathFactory::create_shallow(&out_node);
            p_out->stat();
            dest_exists = p_out->exists();
	      
	    if (!dest_exists) //now we check for temporary files
	    {
		dest_exists = check_temporary(p_work, &out_node);
		printf("in process_stat After check_temporary, dest_exist %d\n", dest_exists);
		if (dest_exists < 0)
		{
			errsend_fmt(FATAL, "Failed to check for temporary files\n");
		}
	    }
            // if selected options require reading the source-file, and the
            // source-file is not readable, we have a problem
            if (((o.work_type == COPYWORK)
                 || ((o.work_type == COMPAREWORK)
                     && ! o.meta_data_only))
                && (! p_work->faccessat(R_OK, AT_SYMLINK_NOFOLLOW))) {

                errsend_fmt(NONFATAL, "No read-access to source-file %s: %s\n",
                            p_work->path(), p_work->strerror());
                process = 0;
            }
            // if selected options require reading the destination-file,
            // and destination-file is not readable, we have a problem
            else if ((((o.work_type == COMPAREWORK)
                       && ! o.meta_data_only))
                     && (! p_out->faccessat(R_OK, AT_SYMLINK_NOFOLLOW))) {

                errsend_fmt(NONFATAL, "No read-access to dest-file %s: %s\n",
                            p_out->path(), p_out->strerror());
                process = 0;
            }
            else if (o.work_type == COPYWORK) {
                process = 1;
                p_work->dest_ftype(p_out->node().ftype); // (matches the intent of old code, above?)
                if (p_out->supports_n_to_1())
                    parallel_dest = 1;
                //if the out path exists
                if (dest_exists == 1) {
                    // Maybe user only wants to operate on source-files
                    // that are "different" from the corresponding
                    // dest-files.
                    if ((o.different == 1)
                        && samefile(p_work, p_out, o)) {

                        process = 0; // source/dest are the same, so skip
                        num_finished_bytes += work_node.st.st_size;
                    }
                    // if someone truncated the destination to zero
                    // (i.e. the only way a zero-size file could have CTM),
                    // then any CTM that may exist is definitely obsolete
                    if (out_node.st.st_size == 0) {
                        purgeCTM(out_node.path);
                    }

                    if (process == 1) {
                            // it's not fuse, unlink
                            // remove the destination-file if the transfer
                            // is unconditional or the source-file size <=
                            // chunk_at size
                            if (! o.different
                                || (work_node.st.st_size <= p_out->chunk_at(o.chunk_at))) {

                                if (! p_out->unlink() && (errno != ENOENT)) {
                                    errsend_fmt(FATAL, "Failed to unlink (2) %s: %s\n",
                                                p_out->path(), p_out->strerror());
                                }
                                out_unlinked = 1; // don't unset the ftype to communicate
                                pre_process = 1;
                            }
                    }
                }
		else if (dest_exists > 1)
		{
			//a temporary file exists
			//we extract timestamp out and put it in work_node
			char timestamp[DATE_STRING_MAX];
			if (get_ctm_timestamp(out_node.path, timestamp) < 0)
			{
				errsend_fmt(FATAL, "Failed to read timestamp for temporary file\n");
			}
			//now we have the time stamp, deal with each accordingly
			if (dest_exists == 2 && o.different == 1)
			{
				//in this case we have a match AND restart is enabled
				//mark work_node with temp flag and copy timestamp
				memcpy(work_node.timestamp, timestamp, DATE_STRING_MAX);
			}
			else if (dest_exists == 3 || o.different == 0)
			{
				//either we have a mismatch in src hash or restart is not on, delete temporary file, and purge CTM
				p_out->create_temporary_path(timestamp);
				if (!p_out->unlink() && (errno != ENOENT))
				{
					errsend_fmt(FATAL, "Failed to unlink temporary file %s\n", p_out->path());
				}
				//now we undo the temorary path
				p_out->restore_original_path();
				
			}
		}
                else {
                    // destination doesn't already exist

                    out_unlinked = 1; // don't unset the ftype to communicate
                    pre_process = 1;

                    // if someone deleted the destination,
                    // then any CTM that may exist is definitely obsolete
                    purgeCTM(out_node.path);
                }
            } // end COPYWORK
            // preping for COMPAREWORK, which means we simply assign the
            // destination type to the source file info
            else if (o.work_type == COMPAREWORK) {
                process = 1;
                work_node.dest_ftype = out_node.ftype;
            }

            if (process == 1) {
                //parallel filesystem can do n-to-1
                if (parallel_dest) {
                    CTM *ctm = (CTM *)NULL;             // CTM structure used with chunked files   
                    // MarFS will adjust a given chunksize to match (some
                    // multiple of) the chunksize of the underlying repo
                    // (the repo matching the file-size), adjusting for the
                    // size of hidden recovery-info that must be written
                    // into each object.  (i.e. chunk_size is smaller than
                    // the actual amount to be written, leaving enough room
                    // for recovery-info)
                    chunk_size = p_out->chunksize(p_work->st().st_size, o.chunksize);
                    chunk_at   = p_out->chunk_at(o.chunk_at);
                    // handle zero-length source file - because it will not
                    // be processed through chunk/file loop below.
                    if (work_node.st.st_size == 0) {
                        pre_process = 1;
                        if ((o.work_type == COPYWORK)
                            && !p_out->unlink()
                            && (errno != ENOENT)) {
                            errsend_fmt(FATAL, "Failed to unlink (3) %s: %s\n",
                                        p_out->path(), p_out->strerror());
                        }
                        out_unlinked = 1;
                        work_node.chkidx = 0;
                        work_node.chksz = 0;
			work_node.packable = 0;
                        regbuffer[reg_buffer_count] = work_node;
                        reg_buffer_count++;
                    }
                    else if (work_node.st.st_size > chunk_at) {     // working with a chunkable file
                        int ctmExist = ((dest_exists) ? hasCTM(out_node.path) : 0);
                        // we are doing a conditional transfer & CTM exists
                        // -> populate CTM structure
                        if (o.different && ctmExists) {
                            ctm = getCTM(out_node.path,
                                         ((long)ceil(work_node.st.st_size / ((double)chunk_size))),
                                         chunk_size);
                            if(IO_DEBUG_ON) {
                                char ctm_flags[2048];
                                char *ctmstr = ctm_flags;
                                int ctmlen = 2048;
                                PRINT_IO_DEBUG("rank %d: process_stat_buffer() "
                                               "Reading persistent store of CTM: %s\n",
                                               rank, tostringCTM(ctm,&ctmstr,&ctmlen));
                            }
                        }
                        else if (ctmExists) {
                            // get rid of the CTM on the file if we are NOT
                            // doing a conditional transfer/compare.
                            purgeCTM(out_node.path);
                            pre_process = 2;
                        }
			work_node.packable = 0;//mark work_node not pacakble
			work_node.temp_flag = 1; //mark work_node needs temporary file due to chunking
                    }
		    else //working with a non-chunkable file, either small enough to be packed, or not packed
		    {
			work_node.packable = p_out->check_packable(work_node.st.st_size);
			printf("output %s is packed? %d\n", p_out->path(), work_node.packable);
			
			if (!work_node.packable)
			{
				work_node.packable = 2; // we identify non chunkable non packable file with packable set to 2
			}
			pre_process = 1;
		    }

                    if (maybe_pre_process(pre_process, o, p_out, p_work)) {
                        errsend_fmt(NONFATAL,
                                    "Rank %d: couldn't prepare destination-file '%s': %s\n",
                                    rank, p_out->path(), ::strerror(errno));
                    }
                    else {
                        // --- CHUNKING-LOOP
                        chunk_curr_offset = 0; // keeps track of current offset in file for chunk.
                        idx = 0;               // keeps track of the chunk index
                        while (chunk_curr_offset < work_node.st.st_size) {
                            work_node.chkidx = idx;         // assign the chunk index
                            // non-chunked file or file is a link or metadata
                            // compare work - just send the whole file
                            if ((work_node.st.st_size <= chunk_at)
                                || (S_ISLNK(work_node.st.st_mode))
                                || (o.work_type == COMPAREWORK && o.meta_data_only)) {
                                work_node.chksz = work_node.st.st_size;   // set chunk size to size of file
                                chunk_curr_offset = work_node.st.st_size; // set chunk offset to end of file
                                PRINT_IO_DEBUG("rank %d: process_stat_buffer() "
                                               "non-chunkable file   chunk index: %d   chunk size: %ld\n",
                                               rank, work_node.chkidx, work_node.chksz);
                            }
                            else {                  // having to chunk the file
                                work_node.chksz = ((ctm) ? ctm->chnksz : chunk_size);
                                chunk_curr_offset += (((chunk_curr_offset + work_node.chksz) >  work_node.st.st_size)
                                                      // should this be (work_node.chksz - chunk_curr_offset)?
                                                      ? (work_node.st.st_size - chunk_curr_offset)
                                                      : work_node.chksz);
                                idx++;
                            }
                                // if a non-conditional transfer or if the
                                // chunk did not make on the first one ...
                                if (!o.different
                                    || !chunktransferredCTM(ctm, work_node.chkidx)) {

                                    num_bytes_seen += work_node.chksz;  // keep track of number of bytes processed
                                    regbuffer[reg_buffer_count] = work_node;// copy source file info into sending buffer
                                    reg_buffer_count++;
                                    PRINT_IO_DEBUG("rank %d: process_stat_buffer() adding chunk "
                                                   "index: %d   chunk size: %ld\n",
                                                   rank, work_node.chkidx, work_node.chksz);
                                    if (((reg_buffer_count % COPYBUFFER) == 0)
                                        || num_bytes_seen >= ship_off) {
                                        PRINT_MPI_DEBUG("rank %d: process_stat_buffer() parallel destination "
                                                        "- sending %d reg buffers to manager.\n",
                                                        rank, reg_buffer_count);
                                        send_manager_regs_buffer(regbuffer, &reg_buffer_count);
                                        num_bytes_seen = 0;
                                    }
                                } // end send test
                                else {
                                    num_finished_bytes += work_node.chksz;
                                }
                        } // end file/chunking loop
                    }
                    // if CTM structure allocated it -> free the memory now
                    if (ctm)
                        freeCTM(&ctm);
                } // end Parallel destination
                else {  // non-parallel destination
                    if (maybe_pre_process(pre_process, o, p_out, p_work)) {
                        errsend_fmt(NONFATAL,
                                    "Rank %d: couldn't prepare destination-file '%s': %s\n",
                                    rank, p_out->path(), ::strerror(errno));
                    }
                    else {
                        work_node.chkidx = 0;           // for non-chunked files, index is always 0
                        work_node.chksz = work_node.st.st_size;     // set chunk size to size of file
                        num_bytes_seen += work_node.chksz;          // send this off to the manager work list, if ready to
                        regbuffer[reg_buffer_count] = work_node;
                        reg_buffer_count++;
                        if (reg_buffer_count % COPYBUFFER == 0 || num_bytes_seen >= ship_off) {
                            PRINT_MPI_DEBUG("rank %d: process_stat_buffer() non-parallel destination "
                                            "- sending %d reg buffers to manager.\n",
                                            rank, reg_buffer_count);
                            send_manager_regs_buffer(regbuffer, &reg_buffer_count);
                            num_bytes_seen = 0;
                        }
                    }
                }
            }
        }

        if (! S_ISDIR(work_node.st.st_mode)) {
            num_examined_files++;
            num_examined_bytes += work_node.st.st_size;
        }

        //if (work_node.st.st_size > 0 && work_node.st.st_blocks == 0){
        if (o.verbose > 1) {

            printmode(work_node.st.st_mode, modebuf);
            memcpy(&sttm, localtime(&work_node.st.st_mtime), sizeof(sttm));
            strftime(timebuf, sizeof(timebuf), "%a %b %d %Y %H:%M:%S", &sttm);
            sprintf(statrecord, "INFO  DATASTAT - %s %6lu %6d %6d %21zd %s %s\n",
                    modebuf, (long unsigned int) work_node.st.st_blocks,
                    work_node.st.st_uid, work_node.st.st_gid,
                    (size_t) work_node.st.st_size, timebuf, work_node.path);

            MPI_Pack(statrecord, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);

            write_count++;
            if (write_count % MESSAGEBUFFER == 0) {
                write_buffer_output(writebuf, writesize, write_count);
                out_position = 0;
                write_count = 0;
            }
        }

        // regbuffer is full (probably with zero-length files) -> send it
        // off to manager. - cds 8/2015
        if (reg_buffer_count >= COPYBUFFER) {
            PRINT_MPI_DEBUG("rank %d: process_stat_buffer() sending %d reg "
                            "buffers to manager.\n", rank, reg_buffer_count);
            send_manager_regs_buffer(regbuffer, &reg_buffer_count);
        }
    } //end of stat processing loop

    //incase we tried to copy a file into itself
    if (0 < write_count && o.verbose >= 1) {
        writesize = MESSAGESIZE * write_count;
        writebuf = (char *) realloc(writebuf, writesize * sizeof(char));
        if (! writebuf) {
            errsend_fmt(FATAL, "Failed to re-allocate %lu bytes for writebuf, write_count: %d\n", writesize, write_count);
        }
        write_buffer_output(writebuf, writesize, write_count);
    }
    
    while(dir_buffer_count != 0) {
        send_manager_dirs_buffer(dirbuffer, &dir_buffer_count);
    }
    
    while (reg_buffer_count != 0) {
        send_manager_regs_buffer(regbuffer, &reg_buffer_count);
    }
 
    send_manager_examined_stats(num_examined_files, num_examined_bytes, num_examined_dirs, num_finished_bytes);
    //free malloc buffers
    free(writebuf);
    *stat_count = 0;
}


//When a worker is told to copy, it comes here
void worker_copylist(int             rank,
                     int             sending_rank,
                     const char*     base_path,
                     path_item*      dest_node,
                     struct options& o) {

    MPI_Status     status;
    char*          workbuf;
    char*          writebuf;
    int            worksize;
    int            writesize;
    int            position;
    int            out_position;
    int            read_count;
    path_item      work_node;
    path_item      out_node;
    char           copymsg[MESSAGESIZE];
    off_t          offset;
    size_t         length;
    int            num_copied_files = 0;
    size_t         num_copied_bytes = 0;
    path_item      chunks_copied[CHUNKBUFFER];
    int            buffer_count = 0;
    int            i;
    int            rc;

    PRINT_MPI_DEBUG("rank %d: worker_copylist() Receiving the read_count from %d\n",
                    rank, sending_rank);
    if (MPI_Recv(&read_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive read_count\n");
    }

    worksize = read_count * sizeof(path_list);
    workbuf = (char *) malloc(worksize * sizeof(char));
    if (! workbuf) {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for workbuf\n", worksize);
    }

    writesize = MESSAGESIZE * read_count;
    writebuf = (char *) malloc(writesize * sizeof(char));
    if (! writebuf) {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for writebuf\n", writesize);
    }

    //gather the path to stat
    PRINT_MPI_DEBUG("rank %d: worker_copylist() Receiving the workbuf from %d\n",
                    rank, sending_rank);
    if (MPI_Recv(workbuf, worksize, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive workbuf\n");
    }

    position = 0;
    out_position = 0;
    for (i = 0; i < read_count; i++) {
        PRINT_MPI_DEBUG("rank %d: worker_copylist() unpacking work_node from %d\n",
                        rank, sending_rank);
        MPI_Unpack(workbuf, worksize, &position, &work_node, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);
        offset = work_node.chkidx*work_node.chksz;
        length = (((offset + work_node.chksz) > work_node.st.st_size)
                  ? (work_node.st.st_size - offset)
                  : work_node.chksz);
        PRINT_MPI_DEBUG("rank %d: worker_copylist() chunk index %d unpacked. "
                        "offset = %ld   length = %ld\n",
                        rank, work_node.chkidx, offset, length);

        get_output_path(&out_node, base_path, &work_node, dest_node, o);
        out_node.fstype = o.dest_fstype; // make sure destination filesystem type is assigned for copy - cds 6/2014

        // Need Path objects for the copy_file at this point ...
        PathPtr p_work( PathFactory::create_shallow(&work_node));
        PathPtr p_out(PathFactory::create_shallow(&out_node));
            rc = copy_file(p_work, p_out, o.blocksize, rank, o);
        if (rc >= 0) {
            if (o.verbose >= 1) {
                if (S_ISLNK(work_node.st.st_mode)) {
                    sprintf(copymsg, "INFO  DATACOPY Created symlink %s from %s\n",
                            out_node.path, work_node.path);
                }
                else {
                    sprintf(copymsg, "INFO  DATACOPY %sCopied %s offs %lld len %lld to %s\n",
                            ((rc == 1) ? "*" : ""),
                            work_node.path, (long long)offset, (long long)length, out_node.path);
                }
                write_output(copymsg, 0);
                out_position = 0;
            }
            num_copied_files +=1;
            if (!S_ISLNK(work_node.st.st_mode)) {
                num_copied_bytes += length;
            }
            //file is chunked
            if (offset != 0 || (offset == 0 && length != work_node.st.st_size)) {
                chunks_copied[buffer_count] = work_node;
                buffer_count++;
            }
        }
    }

    //update the chunk information
    if (buffer_count > 0) {
        send_manager_chunk_busy();
        update_chunk(chunks_copied, &buffer_count);
    }

    if (num_copied_files > 0 || num_copied_bytes > 0) {
        send_manager_copy_stats(num_copied_files, num_copied_bytes);
    }

#ifdef MARFS
   if(!MARFS_Path::close_fh()) {
       errsend_fmt(NONFATAL, "Failed to close file handle\n");
   }
#endif
    send_manager_work_done(rank);
    free(workbuf);
    free(writebuf);
}

//When a worker is told to compare, it comes here
void worker_comparelist(int             rank,
        int             sending_rank,
        const char*     base_path,
        path_item*      dest_node,
        struct options& o) {

    MPI_Status   status;
    char *       workbuf;
    char *       writebuf;
    int          worksize;
    int          writesize;
    int          position;
    int          out_position;
    int          read_count;
    path_item    work_node;
    path_item    out_node;
    char         copymsg[MESSAGESIZE];
    off_t        offset;
    size_t       length;
    int          num_compared_files = 0;
    size_t       num_compared_bytes = 0;
    path_item    chunks_copied[CHUNKBUFFER];
    int          buffer_count = 0;
    int          i;
    int          rc;
    int          output = 0;    // did vebosity-level let us print anything?

    PRINT_MPI_DEBUG("rank %d: worker_copylist() Receiving the read_count from %d\n", rank, sending_rank);
    if (MPI_Recv(&read_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive read_count\n");
    }

    worksize = read_count * sizeof(path_list);
    workbuf = (char *) malloc(worksize * sizeof(char));
    if (! workbuf) {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for workbuf\n", worksize);
    }

    writesize = MESSAGESIZE * read_count;
    writebuf = (char *) malloc(writesize * sizeof(char));
    if (! writebuf) {
        errsend_fmt(FATAL, "Failed to allocate %lu bytes for writebuf\n", writesize);
    }

    //gather the path to stat
    PRINT_MPI_DEBUG("rank %d: worker_copylist() Receiving the workbuf from %d\n", rank, sending_rank);
    if (MPI_Recv(workbuf, worksize, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive workbuf\n");
    }

    position = 0;
    out_position = 0;
    for (i = 0; i < read_count; i++) {
        PRINT_MPI_DEBUG("rank %d: worker_copylist() unpacking work_node from %d\n", rank, sending_rank);
        MPI_Unpack(workbuf, worksize, &position, &work_node, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);

        get_output_path(&out_node, base_path, &work_node, dest_node, o);
        stat_item(&out_node, o);
        offset = work_node.chkidx*work_node.chksz;
        length = work_node.chksz;
        rc = compare_file(&work_node, &out_node, o.blocksize, o.meta_data_only, o);
        if (o.meta_data_only || S_ISLNK(work_node.st.st_mode)) {
            sprintf(copymsg, "INFO  DATACOMPARE compared %s to %s", work_node.path, out_node.path);
        }
        else {
            sprintf(copymsg, "INFO  DATACOMPARE compared %s offs %lld len %lld to %s",
                    work_node.path, (long long)offset, (long long)length, out_node.path);
        }

        if (rc == 0) {
            strncat(copymsg, " -- SUCCESS\n", MESSAGESIZE);
        }
        else if (rc == 2 ) {
            strncat(copymsg, " -- MISSING DESTINATION\n", MESSAGESIZE);
            send_manager_nonfatal_inc();
        }
        else {
            strncat(copymsg, " -- MISMATCH\n", MESSAGESIZE);
            send_manager_nonfatal_inc();
        }

        if ((rc != 0)
            || (o.verbose >= 1)) {
            output = 1;
            MPI_Pack(copymsg, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);
        }

        // file is 'chunked'?
        if (offset != 0 || length != work_node.st.st_size) {
            chunks_copied[buffer_count] = work_node;
            buffer_count++;
        }
        num_compared_files +=1;
        num_compared_bytes += length;
    }
    if (output) {
        write_buffer_output(writebuf, writesize, read_count);
    }

    // update the chunk information
    if (buffer_count > 0) {
        send_manager_chunk_busy();
        update_chunk(chunks_copied, &buffer_count);
    }

    // report stats for all files (i.e. chunked or non-chunked)
    if (num_compared_files > 0 || num_compared_bytes > 0) {
        send_manager_copy_stats(num_compared_files, num_compared_bytes);
    }
    send_manager_work_done(rank);
    free(workbuf);
    free(writebuf);
}
