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
#include <time.h>
#include <syslog.h>

#include "pftool.h"
#include "ctm.h"
#include "Path.h"

#ifdef THREADS_ONLY
#  define MPI_Abort  MPY_Abort
#  define MPI_Pack   MPY_Pack
#  define MPI_Unpack MPY_Unpack
#endif


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
    char src_path[PATHSIZE_PLUS];
    char dest_path[PATHSIZE_PLUS];
    struct stat dest_stat;
    int statrc;

#ifdef S3
    // aws_init() (actually, curl_global_init()) is supposed to be done
    // before *any* threads are created.  Could MPI_Init() create threads
    // (or call multi-threaded libraries)?  We'll assume so.
    AWS4C_CHECK( aws_init() );
    s3_enable_EMC_extensions(1);
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
        o.verbose = 0;
        o.use_file_list = 0;
        o.recurse = 0;
        o.logging = 0;
        o.meta_data_only = 1;
        o.dest_fstype = UNKNOWN_FS;
        strncpy(o.jid, "TestJob", 128);
        o.parallel_dest = 0;
        o.blocksize = (1024 * 1024);
        o.chunk_at  = (100ULL * 1024 * 1024 * 1024); // 107374182400
        o.chunksize = (100ULL * 1024 * 1024 * 1024);

#ifdef FUSE_CHUNKER
        //so we don't make fuse files not on archive
        strncpy(o.archive_path, "", PATHSIZE_PLUS);
        //fuse
        strncpy(o.fuse_path, "", PATHSIZE_PLUS);
        o.use_fuse = 0;
        o.fuse_chunk_at  = (64ULL * 1024 * 1024 * 1024); // 68719476736
        o.fuse_chunksize = (64ULL * 1024 * 1024 * 1024);
        o.fuse_chunkdirs = 10;
#endif

#ifdef PLFS
        o.plfs_chunksize = (100ULL * 1024 * 1024); // 104857600
#endif

        o.work_type = LSWORK;   // default op is to do a listing (not printed)

#ifdef GEN_SYNDATA
        o.syn_pattern[0] = '\0'; // Make sure synthetic data pattern file or name is clear
        o.syn_size = 0;          // Clear the synthetic data size
#endif

        // start MPI - if this fails we cant send the error to thtooloutput proc so we just die now
        while ((c = getopt(argc, argv, "p:c:j:w:i:s:C:S:a:f:d:W:A:t:X:x:z:vrlPMnh")) != -1)
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

                case 'X':
#ifdef GEN_SYNDATA
                    strncpy(o.syn_pattern, optarg, 128);
#else
                    errsend(NONFATAL,"configure with --enable-syndata, to use option '-X'");
#endif
                    break;

                case 'x':
#ifdef GEN_SYNDATA
                    o.syn_size = str2Size(optarg);
#else
                    errsend(NONFATAL,"configure with --enable-syndata, to use option '-x'");
#endif
                    break;

#ifdef FUSE_CHUNKER
                case 'a':
                    strncpy(o.archive_path, optarg, PATHSIZE_PLUS);
                    break;
                case 'f':
                    strncpy(o.fuse_path, optarg, PATHSIZE_PLUS);
                    o.use_fuse = 1;
                    break;
                case 'd':
                    o.fuse_chunkdirs = atoi(optarg);
                    break;
                case 'W':
                    o.fuse_chunk_at = str2Size(optarg);
                    break;
                case 'A':
                    o.fuse_chunksize = str2Size(optarg);
                    break;
#endif

#ifdef PLFS
                case 'z':
                    o.plfs_chunksize = str2Size(optarg);
                    break;
#endif

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
                    //  >= 1  means normal diagnostics
                    //  >= 2  means ...
                    //  >= 3  means also show S3 client/server interaction
                    o.verbose += 1;
                    break;

                case 'h':
                    //Help -- incoming!
                    usage();
                    return 0;
                case '?':
                    return -1;
                default:
                    break;
            }
    }
    MPI_Barrier(MPI_COMM_WORLD);


    if (o.verbose && (rank == MANAGER_PROC)) {
        printf("ranks = %d\n", nproc);
    }
    // assure the minimal number of ranks exist
    if (nproc <= START_PROC) {
        fprintf(stderr, "Requires at least %d ranks\n", START_PROC +1);
        return -1;
    }

    //broadcast all the options
    MPI_Bcast(&o.verbose, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
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

#ifdef FUSE_CHUNKER
    MPI_Bcast(o.archive_path, PATHSIZE_PLUS, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(o.fuse_path, PATHSIZE_PLUS, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.use_fuse, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.fuse_chunkdirs, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.fuse_chunk_at, 1, MPI_DOUBLE, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.fuse_chunksize, 1, MPI_DOUBLE, MANAGER_PROC, MPI_COMM_WORLD);
#endif

#ifdef PLFS
    MPI_Bcast(&o.plfs_chunksize, 1, MPI_DOUBLE, MANAGER_PROC, MPI_COMM_WORLD);
#endif

    MPI_Bcast(&o.use_file_list, 1, MPI_INT, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(o.jid, 128, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);

#ifdef GEN_SYNDATA
    MPI_Bcast(o.syn_pattern, 128, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
    MPI_Bcast(&o.syn_size, 1, MPI_DOUBLE, MANAGER_PROC, MPI_COMM_WORLD);
#endif


    // Path factory might want to use some of these fields.
    // TBD: Maybe we also want the src files processed via enqueue_path(), below.
    // 
    PathFactory::initialize(&o, rank, src_path, dest_path);

    // providing multiple '-v' args on the command line increases the value
    // of o.verbose.  Any non-zero value turns on verbosity.  However, if
    // o.verbose > 1, we also sleep for 5 seconds here.  That allows a user
    // to attach gdb to the running process.  We have a script that can do
    // this quickly.
    if (o.verbose > 1) {
        fprintf(stderr, "sleeping to allow gdb to attach ... ");
        fflush(stdout);
        sleep(5);
        fprintf(stderr, "done.\n");

        MPI_Barrier(MPI_COMM_WORLD);
    }

    //freopen( "/dev/null", "w", stderr );
    //Modifies the path based on recursion/wildcards
    //wildcard
    if (rank == MANAGER_PROC) {

        if (optind < argc && (o.work_type == COPYWORK || o.work_type == COMPAREWORK)) {
            statrc = lstat(dest_path, &dest_stat);
            if (statrc < 0 || !S_ISDIR(dest_stat.st_mode)) {
                printf("Multiple inputs and target '%s' is not a directory\n", dest_path);
                MPI_Abort(MPI_COMM_WORLD, -1);
            }
        }
        if ((optind < argc) && (o.use_file_list)) { // only one of them is enqueued, below
            printf("Provided sources via '-i' and on the command-line\n");
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
        else if (o.use_file_list)
            enqueue_path(&input_queue_head, &input_queue_tail, o.file_list, &input_queue_count);
        else
            enqueue_path(&input_queue_head, &input_queue_tail, src_path, &input_queue_count);
    }

    // take on the role appropriate to our rank.
    if (rank == MANAGER_PROC) {
        manager(rank, o, nproc, input_queue_head, input_queue_tail, input_queue_count, dest_path);
    }
    else {
        // one of these workers is the OUTPUT_PROC.  Wouldn't it make sense
        // for him to sit out the worker() task?  Otherwise we deadlock if
        // we use errsend() when he's e.g. doing a Bcast, as a worker.
        // [Alternatively, errsend() could be asynchronous, but then you
        // may have output that never reaches the user.]
        worker(rank, o);
    }

    //Program Finished
    //printf("%d -- done.\n", rank);
    MPI_Finalize();
    return 0;
}


void manager(int             rank,
        struct options& o,
        int             nproc,
        path_list*      input_queue_head,
        path_list*      input_queue_tail,
        int             input_queue_count,
        const char*     dest_path) {

    MPI_Status  status;

#ifndef THREADS_ONLY
    int         message_ready = 0;
    int         probecount = 0;
    int         prc;
#endif

    int         type_cmd;
    int         work_rank;
    int         sending_rank;
    int         i;
    int*        proc_status;

    struct timeval in;
    struct timeval out;

    int         non_fatal = 0;

    int         examined_file_count = 0;
    int         examined_dir_count = 0;
    size_t      examined_byte_count = 0;
#ifdef TAPE
    int         examined_tape_count = 0;
    size_t      examined_tape_byte_count = 0;
#endif

    char        message[MESSAGESIZE];
    char        errmsg[MESSAGESIZE];
    char        base_path[PATHSIZE_PLUS];
    char        temp_path[PATHSIZE_PLUS];

    struct stat st;

    path_item   beginning_node;
    path_item   dest_node;
    path_list*  iter = NULL;
    int         num_copied_files = 0;
    size_t      num_copied_bytes = 0;

    work_buf_list* stat_buf_list      = NULL;
    int            stat_buf_list_size = 0;

    work_buf_list* process_buf_list   = NULL;
    int            process_buf_list_size = 0;

    work_buf_list* dir_buf_list       = NULL;
    int            dir_buf_list_size  = 0;
#ifdef TAPE
    work_buf_list* tape_buf_list      = NULL;
    int            tape_buf_list_size = 0;
#endif

    int         mpi_ret_code;
    int         rc;
    int         start = 1;

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

    if (!o.use_file_list) { // If not using a file list -> broadcast the destination path
        //setup paths
        strncpy(beginning_node.path, input_queue_head->data.path, PATHSIZE_PLUS);
        get_base_path(base_path, &beginning_node, wildcard);
        if (o.work_type != LSWORK) {

            //need to stat_item sooner, we're doing a mkdir we shouldn't be doing, here.
            rc = stat_item(&beginning_node, o);
            get_dest_path(&dest_node, dest_path, &beginning_node, makedir, input_queue_count, o);
            ////            rc = stat_item(&dest_node, o); // now done in get_dest_path, via Factory

            if (S_ISDIR(beginning_node.st.st_mode) && makedir == 1){
                //// #ifdef PLFS
                ////                 if (dest_node.ftype == PLFSFILE){
                ////                     plfs_mkdir(dest_node.path, S_IRWXU);
                ////                 }
                ////                 else {
                //// #endif
                ////                     mkdir(dest_node.path, S_IRWXU);
                //// #ifdef PLFS
                ////                 }
                //// #endif
                ////                 rc = stat_item(&dest_node, o);


                // NOTE: If we errsend anything here, we'll deadlock on the
                //       other procs that are waiting at the Bcast().
                //       That's because there's a problem witht he way
                //       OUTPUT_PROC is used.  Either the errsend()
                //       functions should send asynchronously, or
                //       OUTPUT_PROC should run a special process (other
                //       than worker()), so that it does nothing but
                //       synchronous recevs of diagnostic messages OUTCMD
                //       and LOGCMD.

                //                errsend_fmt(NONFATAL, "Debugging: dest_node '%s' -> dest_path '%s'\n",
                //                            dest_node.path, dest_path);
                fprintf(stderr, "Debugging: dest_path '%s' -> dest_node '%s'\n", dest_path, dest_node.path);

                PathPtr p(PathFactory::create_shallow(&dest_node));
                //                errsend_fmt(NONFATAL, "Debugging: Path subclass is '%s'\n", p->path());
                fprintf(stderr, "Debugging: Path subclass is '%s'\n", p->class_name().get());

                p->mkdir(S_IRWXU);
                //                errsend_fmt(NONFATAL, "Debugging: created '%s'\n", p->path());
                fprintf(stderr, "Debugging: created directory '%s'\n", p->path());

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

            //PRINT_MPI_DEBUG("rank %d: manager() MPI_Bcast the dest_path: %s\n", rank, dest_path);
            mpi_ret_code = MPI_Bcast(&dest_node, sizeof(path_item), MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
            if (mpi_ret_code < 0) {
                errsend(FATAL, "Failed to Bcast dest_path");
            }
        }
        //PRINT_MPI_DEBUG("rank %d: manager() MPI_Bcast the base_path: %s\n", rank, base_path);
        mpi_ret_code = MPI_Bcast(base_path, PATHSIZE_PLUS, MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
        if (mpi_ret_code < 0) {
            errsend(FATAL, "Failed to Bcast base_path");
        }
    }

    // Make sure there are no multiple roots for a recursive operation
    // (because we assume we can use base_path to generate all destination paths?)
    // (because multiple roots imply recursive descent will iterate forever?)
    iter = input_queue_head;
    if (strncmp(base_path, ".", PATHSIZE_PLUS) != 0 && o.recurse == 1 && o.work_type != LSWORK) {
        char iter_base_path[PATHSIZE_PLUS];
        while (iter != NULL) {
            get_base_path(iter_base_path, &(iter->data), wildcard);
            if (strncmp(iter_base_path, base_path, PATHSIZE_PLUS) != 0) {
                errsend(FATAL, "All sources for a recursive operation must be contained within the same directory.");
            }
            iter = iter->next;
        }
    }

    //quick check that source is not nested
    char* copy = strdup(dest_path);
    strncpy(temp_path, dirname(copy), PATHSIZE_PLUS);
    free(copy);
    ////    rc = stat(temp_path, &st);
    ////    if (rc < 0)
    PathPtr p_dir(PathFactory::create((char*)temp_path));
    if (! p_dir->exists())

    {
        char err_cause[MESSAGESIZE];
        strerror_r(errno, err_cause, MESSAGESIZE);
        snprintf(errmsg, MESSAGESIZE, "%s: %s", dest_path, err_cause);
        errsend(FATAL, errmsg);
    }

    //pack our list into a buffer:
    pack_list(input_queue_head, input_queue_count, &dir_buf_list, &dir_buf_list_size);
    delete_queue_path(&input_queue_head, &input_queue_count);

    //allocate a vector to hold proc status for every proc
    proc_status = (int*)malloc(nproc * sizeof(int));

    //initialize proc_status
    for (i = 0; i < nproc; i++) {
        proc_status[i] = 0;
    }
    sprintf(message, "INFO  HEADER   ========================  %s  ============================\n", o.jid);
    write_output(message, 1);
    sprintf(message, "INFO  HEADER   Starting Path: %s\n", beginning_node.path);
    write_output(message, 1);

    //starttime
    gettimeofday(&in, NULL);

    //this is how we start the whole thing
    proc_status[START_PROC] = 1;
    send_worker_readdir(START_PROC, &dir_buf_list, &dir_buf_list_size);

    // process responses from workers
    while (1) {

        //poll for message
#ifndef THREADS_ONLY
        while ( message_ready == 0) {
            prc = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &message_ready, &status);
            if (prc != MPI_SUCCESS) {
                errsend(FATAL, "MPI_Iprobe failed\n");
                message_ready = -1;
            }
            else {
                probecount++;
            }
            if  (probecount % 3000 == 0) {
                PRINT_POLL_DEBUG("Rank %d: Waiting for a message\n", rank);
                PRINT_POLL_DEBUG("process_buf_list_size = %d\n", process_buf_list_size);
                PRINT_POLL_DEBUG("stat_buf_list_size = %d\n", stat_buf_list_size);
                PRINT_POLL_DEBUG("dir_buf_list_size = %d\n", dir_buf_list_size);
            }
            //we didn't get any new messages from workers
            if (message_ready == 0) {
#endif
                for (i = 0; i < nproc; i++) {
                    PRINT_PROC_DEBUG("Rank %d, Status %d\n", i, proc_status[i]);
                }
                PRINT_PROC_DEBUG("=============\n");
                work_rank = get_free_rank(proc_status, START_PROC, nproc - 1);
                if (work_rank >= 0) {
                    if (((start == 1 || o.recurse) && dir_buf_list_size != 0) ||
                            (o.use_file_list && dir_buf_list_size != 0 && stat_buf_list_size < nproc*3)) {
                        proc_status[work_rank] = 1;
                        send_worker_readdir(work_rank, &dir_buf_list, &dir_buf_list_size);
                        start = 0;
                    }
                    else if (!o.recurse) {
                        delete_buf_list(&dir_buf_list, &dir_buf_list_size);
                    }
                }
#ifdef TAPE
                //handle tape
                work_rank = get_free_rank(proc_status, START_PROC, nproc - 1);
                if (work_rank >= 0 && tape_buf_list_size > 0) {
                    proc_status[work_rank] = 1;
                    send_worker_tape_path(work_rank, &tape_buf_list, &tape_buf_list_size);
                }
#endif
                if (o.work_type == COPYWORK) {
                    for (i = 0; i < 3; i ++) {
                        work_rank = get_free_rank(proc_status, START_PROC, nproc - 1);
                        if (work_rank >= 0 && process_buf_list_size > 0) {
                            proc_status[work_rank] = 1;
                            send_worker_copy_path(work_rank, &process_buf_list, &process_buf_list_size);
                        }
                    }
                }
                else if (o.work_type == COMPAREWORK) {
                    for (i = 0; i < 3; i ++) {
                        work_rank = get_free_rank(proc_status, START_PROC, nproc - 1);
                        if (work_rank >= 0 && process_buf_list_size > 0) {
                            proc_status[work_rank] = 1;
                            send_worker_compare_path(work_rank, &process_buf_list, &process_buf_list_size);
                        }
                    }
                }
                else {
                    //delete the queue here
                    delete_buf_list(&process_buf_list, &process_buf_list_size);
#ifdef TAPE
                    delete_buf_list(&tape_buf_list, &tape_buf_list_size);
#endif
                }
#ifndef THREADS_ONLY
            }

            //are we finished?
            if (process_buf_list_size == 0 &&
                    stat_buf_list_size == 0 &&
                    dir_buf_list_size == 0 &&
                    processing_complete(proc_status, nproc) == 0) {
                break;
            }
            usleep(1);
        }
#endif

        // got a message, or nothing left to do
        if (process_buf_list_size == 0 &&
                stat_buf_list_size == 0 &&
                dir_buf_list_size == 0 &&
                processing_complete(proc_status, nproc) == 0) {
            break;
        }

        // got a message, get message type
        if (MPI_Recv(&type_cmd, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
            errsend(FATAL, "Failed to receive type_cmd\n");
        }
        sending_rank = status.MPI_SOURCE;
        PRINT_MPI_DEBUG("rank %d: manager() Receiving the command %s from rank %d\n", rank, cmd2str(type_cmd), sending_rank);
        //do operations based on the message
        switch(type_cmd) {
            case WORKDONECMD:
                //worker finished their tasks
                manager_workdone(rank, sending_rank, proc_status);
                break;

            case NONFATALINCCMD:
                //non fatal errsend encountered
                non_fatal++;
                break;

            case CHUNKBUSYCMD:
                proc_status[ACCUM_PROC] = 1;
                break;

            case COPYSTATSCMD:
                manager_add_copy_stats(rank, sending_rank, &num_copied_files, &num_copied_bytes);
                break;

            case EXAMINEDSTATSCMD:
                manager_add_examined_stats(rank, sending_rank, &examined_file_count, &examined_byte_count, &examined_dir_count);
                break;
#ifdef TAPE
            case TAPESTATCMD:
                manager_add_tape_stats(rank, sending_rank, &examined_tape_count, &examined_tape_byte_count);
                break;
#endif
            case PROCESSCMD:
                manager_add_buffs(rank, sending_rank, &process_buf_list, &process_buf_list_size);
                break;

            case DIRCMD:
                manager_add_buffs(rank, sending_rank, &dir_buf_list, &dir_buf_list_size);
                break;
#ifdef TAPE
            case TAPECMD:
                manager_add_buffs(rank, sending_rank, &tape_buf_list, &tape_buf_list_size);
                if (o.work_type == LSWORK) {
                    delete_buf_list(&tape_buf_list, &tape_buf_list_size);
                }
                break;
#endif
            case INPUTCMD:
                manager_add_buffs(rank, sending_rank, &stat_buf_list, &stat_buf_list_size);
                break;

            case QUEUESIZECMD:
                send_worker_queue_count(sending_rank, stat_buf_list_size);
                break;

            default:
                break;
        }
#ifndef THREADS_ONLY
        message_ready = 0;
#endif
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
#ifdef TAPE
    sprintf(message, "INFO  FOOTER   Total Files on Tape: %d\n", examined_tape_count);
    write_output(message, 1);
    sprintf(message, "INFO  FOOTER   Total Bytes on Tape: %zd\n", examined_tape_byte_count);
    write_output(message, 1);
#endif
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
    //free any allocated stuff
    free(proc_status);
}

// recv <path_count>, then a block of packed data.  Unpack to individual path_items, pushing onto tail of queue
int manager_add_paths(int rank, int sending_rank, path_list **queue_head, path_list **queue_tail, int *queue_count) {
    MPI_Status  status;
    int         path_count;
    path_list*  work_node = (path_list*)malloc(sizeof(path_list));
    char        path[PATHSIZE_PLUS];
    char *      workbuf;
    int         worksize;
    int         position;
    int         i;

    //gather the # of files
    PRINT_MPI_DEBUG("rank %d: manager_add_paths() Receiving path_count from rank %d\n", rank, sending_rank);
    if (MPI_Recv(&path_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive path_count\n");
    }
    worksize =  path_count * sizeof(path_list);
    workbuf  = (char *) malloc(worksize * sizeof(char));

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
void manager_add_buffs(int rank, int sending_rank, work_buf_list **workbuflist, int *workbufsize) {
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

    //gather the path to stat
    PRINT_MPI_DEBUG("rank %d: manager_add_buffs() Receiving buff from rank %d\n", rank, sending_rank);
    if (MPI_Recv(workbuf, worksize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }
    if (path_count > 0) {
        enqueue_buf_list(workbuflist, workbufsize, workbuf, path_count);
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

void manager_add_examined_stats(int rank, int sending_rank, int *num_examined_files, size_t *num_examined_bytes, int *num_examined_dirs) {
    MPI_Status status;
    int        num_files = 0;
    size_t     num_bytes = 0;
    int        num_dirs = 0;

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
    *num_examined_files += num_files;
    *num_examined_bytes += num_bytes;
    *num_examined_dirs += num_dirs;
}

#ifdef TAPE
void manager_add_tape_stats(int rank, int sending_rank, int *num_examined_tapes, size_t *num_examined_tape_bytes) {
    MPI_Status status;
    int num_tapes = 0;
    size_t  num_bytes = 0;
    PRINT_MPI_DEBUG("rank %d: manager_add_examined_stats() Receiving num_examined_tapes from rank %d\n", rank, sending_rank);
    if (MPI_Recv(&num_tapes, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }
    PRINT_MPI_DEBUG("rank %d: manager_add_examined_stats() Receiving num_examined_bytes from rank %d\n", rank, sending_rank);
    if (MPI_Recv(&num_bytes, 1, MPI_DOUBLE, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }
    *num_examined_tapes += num_tapes;
    *num_examined_tape_bytes += num_bytes;
}
#endif

void manager_workdone(int rank, int sending_rank, int *proc_status) {
    proc_status[sending_rank] = 0;
}

void worker(int rank, struct options& o) {
    MPI_Status status;
    int sending_rank;
    int all_done = 0;
    int makedir = 0;

#ifndef THREADS_ONLY
    int message_ready = 0, probecount = 0;
    int prc;
#endif

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


    if (rank == OUTPUT_PROC) {
        output_buffer = (char *) malloc(MESSAGEBUFFER * MESSAGESIZE * sizeof(char));
        memset(output_buffer, '\0', sizeof(MESSAGEBUFFER * MESSAGESIZE));
    }
    if (rank == ACCUM_PROC) {
        if(!(chunk_hash=hashtbl_create(base_count, NULL))) {
            errsend(FATAL, "hashtbl_create() failed\n");
        }
    }
    if (o.work_type == COPYWORK) {
        makedir = 1;
    }
    if (!o.use_file_list) {
        //PRINT_MPI_DEBUG("rank %d: worker() MPI_Bcast the dest_path\n", rank);
        if (o.work_type != LSWORK) {
            mpi_ret_code = MPI_Bcast(&dest_node, sizeof(path_item), MPI_CHAR, MANAGER_PROC, MPI_COMM_WORLD);
            if (mpi_ret_code < 0) {
                errsend(FATAL, "Failed to Receive Bcast dest_path");
            }
        }
        //PRINT_MPI_DEBUG("rank %d: worker() MPI_Bcast the base_path\n", rank);
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
    }

    //This should only be done once and by one proc to get everything started
    if (rank == START_PROC) {
        if (MPI_Recv(&type_cmd, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
            errsend(FATAL, "Failed to receive type_cmd\n");
        }
        sending_rank = status.MPI_SOURCE;
        PRINT_MPI_DEBUG("rank %d: worker() Receiving the command %s from rank %d\n", rank, cmd2str(type_cmd), sending_rank);
        worker_readdir(rank, sending_rank, base_path, &dest_node, 1, makedir, o);
        //TODO: Check worker_readdir(rank, sending_rank, base_path, dest_node, 1, makedir, o);
    }

    //change this to get request first, process, then get work
    while ( all_done == 0) {

#ifndef THREADS_ONLY
        //poll for message
        while ( message_ready == 0) {
            prc = MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &message_ready, &status);
            if (prc != MPI_SUCCESS) {
                errsend(FATAL, "MPI_Iprobe failed\n");
            }
            else {
                probecount++;
            }
            if  (probecount % 3000 == 0) {
                PRINT_POLL_DEBUG("Rank %d: Waiting for a message\n", rank);
            }
            usleep(1);
        }
#endif
        //grab message type
        if (MPI_Recv(&type_cmd, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
            errsend(FATAL, "Failed to receive type_cmd\n");
        }
        sending_rank = status.MPI_SOURCE;
        PRINT_MPI_DEBUG("rank %d: worker() Receiving the type_cmd %s from rank %d\n", rank, cmd2str(type_cmd), sending_rank);
        //do operations based on the message
        switch(type_cmd) {
            case OUTCMD:
                worker_output(rank, sending_rank, 0, output_buffer, &output_count, o);
                break;
            case BUFFEROUTCMD:
                worker_buffer_output(rank, sending_rank, output_buffer, &output_count, o);
                break;
            case LOGCMD:
                worker_output(rank, sending_rank, 1, output_buffer, &output_count, o);
                break;
            case UPDCHUNKCMD:
                worker_update_chunk(rank, sending_rank, &chunk_hash, &hash_count, base_path, &dest_node, o);
                break;
            case DIRCMD:
                worker_readdir(rank, sending_rank, base_path, &dest_node, 0, makedir, o);
                break;
#ifdef TAPE
            case TAPECMD:
                worker_taperecall(rank, sending_rank, &dest_node, o);
                break;
#endif
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
#ifndef THREADS_ONLY
        message_ready = 0;
#endif
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
 * @param base_path  ??
 * @param dest_node  ??
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
    HASHDATA      *hash_value;
    size_t      chunk_size;
    int         i;

    //gather the # of files
    if (MPI_Recv(&path_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive path_count\n");
    }
    PRINT_MPI_DEBUG("rank %d: worker_update_chunk() Receiving path_count from rank %d (path_count = %d)\n", rank, sending_rank,path_count);
    worksize =  path_count * sizeof(path_list);
    workbuf = (char *) malloc(worksize * sizeof(char));
    //gather the path to stat
    PRINT_MPI_DEBUG("rank %d: manager_add_paths() Receiving worksize from rank %d\n", rank, sending_rank);
    //get the work nodes
    if (MPI_Recv(workbuf, worksize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive worksize\n");
    }
    position = 0;
    for (i = 0; i < path_count; i++) {
        MPI_Unpack(workbuf, worksize, &position, &work_node, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);

        PRINT_MPI_DEBUG("rank %d: worker_update_chunk() Unpacking the work_node from rank %d (chunk %d of file %s)\n", rank, sending_rank, work_node.chkidx, work_node.path);

        hash_value = hashtbl_get(*chunk_hash, work_node.path);
        chunk_size = work_node.length;
        if (hash_value == (HASHDATA *)NULL) {

            //resize the hashtable if needed
            if (*hash_count == (*chunk_hash)->size) {
                hashtbl_resize(*chunk_hash, *hash_count+100);
            }
            *hash_count += 1;

            if(hash_value = hashdata_create(work_node)) {
                hashtbl_insert(*chunk_hash, work_node.path, hash_value);
                hashdata_update(hash_value,work_node);                        // make sure the new structure has recorded this chunk!
            }
        } else {                                          // --- Structure for File needs to be updated
            hashdata_update(hash_value,work_node);                      // this will update the data in the table
            if(IO_DEBUG_ON) {
                char ctm_flags[2048];
                char *ctmstr = ctm_flags;
                int ctmlen = 2048;

                PRINT_IO_DEBUG("rank %d: worker_update_chunk() Updating CTM (chunk %d of file %s)\n%s\n", rank, work_node.chkidx, work_node.path,tostringCTM((CTM *)hash_value,&ctmstr,&ctmlen));
            }
        }

        if (hash_value == (HASHDATA *)NULL) {                            // if no hash_value at this point, we have a problem!
            errsend(NONFATAL, "Do not have a hashed data structure for a chunked file!\n");
        } else if (hashdata_filedone(hash_value)) {                       // --- File is done transferring
            PRINT_IO_DEBUG("rank %d: worker_update_chunk() Last Chunk transferred. CTM should be removed. (chunk %d of file %s)\n", rank, work_node.chkidx, work_node.path);
            hash_value = hashtbl_remove(*chunk_hash, work_node.path);               // remove structure for File from hash table
            hashdata_destroy(&hash_value);                          // we are done with the data
            get_output_path(out_node.path, base_path, &work_node, dest_node, o);
            update_stats(&work_node, &out_node);
        }
    }
    free(workbuf);
    send_manager_work_done(rank);
}

void worker_output(int rank, int sending_rank, int log, char *output_buffer, int *output_count, struct options& o) {
    //have a worker receive and print a single message
    MPI_Status status;
    char msg[MESSAGESIZE];
    char sysmsg[MESSAGESIZE + 50];

    //gather the message to print
    if (MPI_Recv(msg, MESSAGESIZE, MPI_CHAR, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive msg\n");
    }
    PRINT_MPI_DEBUG("rank %d: worker_output() Receiving the message from rank %d\n", rank, sending_rank);
    if (o.logging == 1 && log == 1) {
        openlog ("PFTOOL-LOG", LOG_PID | LOG_CONS, LOG_USER);
        sprintf(sysmsg, "[pftool] [%s] - %s", o.jid, msg);
        syslog (LOG_ERR | LOG_USER, "%s", sysmsg);
        closelog();
    }
    if (sending_rank == MANAGER_PROC){
        printf("%s", msg);
    }
    else{
        printf("RANK %3d: %s", sending_rank, msg);
    }
    fflush(stdout);
}

void worker_buffer_output(int rank, int sending_rank, char *output_buffer, int *output_count, struct options& o) {

    //have a worker receive and print a single message
    MPI_Status status;
    int message_count;
    char msg[MESSAGESIZE];
    //char outmsg[MESSAGESIZE+10];
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

    //gather the path to stat
    PRINT_MPI_DEBUG("rank %d: worker_buffer_output() Receiving the buffer from %d\n", rank, sending_rank);
    if (MPI_Recv(buffer, buffersize, MPI_PACKED, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive buffer\n");
    }
    position = 0;
    for (i = 0; i < message_count; i++) {
        PRINT_MPI_DEBUG("rank %d: worker_buffer_output() Unpacking the message from %d\n", rank, sending_rank);
        MPI_Unpack(buffer, buffersize, &position, msg, MESSAGESIZE, MPI_CHAR, MPI_COMM_WORLD);
        //snprintf(outmsg, MESSAGESIZE+10, "RANK %3d: %s", sending_rank, msg);
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
    char        mkdir_path[PATHSIZE_PLUS];
    path_item   work_node;
    path_item   workbuffer[STATBUFFER];
    int         buffer_count = 0;

    DIR*           dip;
    struct dirent* dit;

#ifdef PLFS
    char        dname[PATHSIZE_PLUS];
    Plfs_dirp * pdirp;
#endif

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

        if (start == 1 && o.use_file_list == 0) {

            //first time through, not using a filelist

            ////            rc = stat_item(&work_node, o);
            ////            if (rc != 0) {
            ////                snprintf(errmsg, MESSAGESIZE, "Failed to stat path %s", work_node.path);
            ////                if (o.work_type == LSWORK) {
            ////                    errsend(NONFATAL, errmsg);
            ////                    return;
            ////                }
            ////                else {
            ////                    errsend(FATAL, errmsg);
            ////                }
            ////            }
            if (! p_work->exists()) { // performs a stat()
                errsend_fmt(((o.work_type == LSWORK) ? NONFATAL : FATAL),
                        "Failed to stat path (1) %s\n", p_work->path());
                if (o.work_type == LSWORK)
                    return;
            }
            workbuffer[buffer_count] = work_node;
            buffer_count++;
        }

        else if (o.use_file_list == 0) {
            // work_node is a source-directory.  Read file-names from it,
            // construct full source-side pathnames.  Eventually these go
            // to process_stat_buffer(), where they are converted to
            // destination-paths.


            ////#ifdef PLFS
            ////            if (work_node.ftype == PLFSFILE){
            ////                if ((rc = plfs_opendir_c(work_node.path,&pdirp)) != 0){
            ////                    snprintf(errmsg, MESSAGESIZE, "Failed to open plfs dir %s\n", work_node.path);
            ////                    errsend(NONFATAL, errmsg);
            ////                    continue;
            ////                }
            ////            }
            ////
            ////            else{
            ////#endif
            ////                if ((dip = opendir(work_node.path)) == NULL) {
            ////                    snprintf(errmsg, MESSAGESIZE, "Failed to open dir %s\n", work_node.path);
            ////                    errsend(NONFATAL, errmsg);
            ////                    continue;
            ////                }
            ////#ifdef PLFS
            ////            }
            ////#endif
            if (! p_work->opendir()) {
                errsend_fmt(NONFATAL, "Failed to open (%s) dir %s\n", 
                        p_work->class_name().get(), p_work->path());
            }


            if (makedir == 1) {
                //get_output_path(mkdir_path, base_path, &work_node, dest_node, o);
                get_output_path(mkdir_path, base_path, &p_work->node(), dest_node, o);

                ////#ifdef PLFS
                ////                struct stat st_temp;
                ////                char* copy = strdup(mkdir_path); // possibly-altered by dirname()
                ////                rc = plfs_getattr(NULL, dirname(copy), &st_temp, 0);
                ////                free(copy);
                ////                if (rc == 0){
                ////                    plfs_mkdir(mkdir_path, S_IRWXU);
                ////                }
                ////                else{
                ////#endif
                ////                    mkdir(mkdir_path, S_IRWXU);
                ////#ifdef PLFS
                ////                }
                ////#endif
                PathPtr p_dir(PathFactory::create(mkdir_path));
                p_dir->mkdir(S_IRWXU);
            }
            // strncpy(path, work_node.path, PATHSIZE_PLUS);
            strncpy(path, p_work->path(), PATHSIZE_PLUS);


            //we're not a file list
            ////#ifdef PLFS
            ////            if (work_node.ftype == PLFSFILE){
            ////                while (1) {
            ////                    rc = plfs_readdir_c(pdirp, dname, PATHSIZE_PLUS);
            ////                    if (rc != 0){
            ////                        snprintf(errmsg, MESSAGESIZE, "Failed to plfs_readdir path %s", work_node.path);
            ////                        errsend(NONFATAL, errmsg);
            ////                        break;
            ////                    }
            ////                    if (strlen(dname) == 0){
            ////                        break;
            ////                    }
            ////                    if (strncmp(dname, ".", PATHSIZE_PLUS) != 0 && strncmp(dname, "..", PATHSIZE_PLUS) != 0) {
            ////                        strncpy(full_path, path, PATHSIZE_PLUS);
            ////                        if (full_path[strlen(full_path) - 1 ] != '/') {
            ////                            strncat(full_path, "/", 1);
            ////                        }
            ////                        strncat(full_path, dname, PATHSIZE_PLUS - strlen(full_path) - 1);
            ////                        strncpy(work_node.path, full_path, PATHSIZE_PLUS);
            ////                        rc = stat_item(&work_node, o);
            ////                        if (rc != 0) {
            ////                            snprintf(errmsg, MESSAGESIZE, "Failed to stat path %s", work_node.path);
            ////                            if (o.work_type == LSWORK) {
            ////                                errsend(NONFATAL, errmsg);
            ////                                continue;
            ////                            }
            ////                            else {
            ////                                errsend(FATAL, errmsg);
            ////                            }
            ////                        }
            ////                        workbuffer[buffer_count] = work_node;
            ////                        buffer_count++;
            ////                        if (buffer_count != 0 && buffer_count % STATBUFFER == 0) {
            ////                            process_stat_buffer(workbuffer, &buffer_count, base_path, dest_node, o, rank);
            ////                        }
            ////                    }
            ////                }
            ////            }
            ////            else{
            ////#endif
            ////                while ((dit = readdir(dip)) != NULL) {
            ////                    if (strncmp(dit->d_name, ".", PATHSIZE_PLUS) != 0 && strncmp(dit->d_name, "..", PATHSIZE_PLUS) != 0) {
            ////                        strncpy(full_path, path, PATHSIZE_PLUS);
            ////                        if (full_path[strlen(full_path) - 1 ] != '/') {
            ////                            strncat(full_path, "/", 1);
            ////                        }
            ////                        strncat(full_path, dit->d_name, PATHSIZE_PLUS - strlen(full_path) - 1);
            ////                        strncpy(work_node.path, full_path, PATHSIZE_PLUS);
            ////                        rc = stat_item(&work_node, o);
            ////                        if (rc != 0) {
            ////                            snprintf(errmsg, MESSAGESIZE, "Failed to stat path %s", work_node.path);
            ////                            if (o.work_type == LSWORK) {
            ////                                errsend(NONFATAL, errmsg);
            ////                                continue;
            ////                            }
            ////                            else {
            ////                                errsend(FATAL, errmsg);
            ////                            }
            ////                        }
            ////                        workbuffer[buffer_count] = work_node;
            ////                        buffer_count++;
            ////                        if (buffer_count != 0 && buffer_count % STATBUFFER == 0) {
            ////                            process_stat_buffer(workbuffer, &buffer_count, base_path, dest_node, o, rank);
            ////                        }
            ////                    }
            ////                }
            ////#ifdef PLFS
            ////            }
            ////            if (work_node.ftype == PLFSFILE){
            ////                if (plfs_closedir_c(pdirp) != 0) {
            ////                    snprintf(errmsg, MESSAGESIZE, "Failed to plfs_closedir: %s", work_node.path);
            ////                    errsend(1, errmsg);
            ////                }
            ////
            ////            }
            ////            else{
            ////#endif
            ////                if (closedir(dip) == -1) {
            ////                    snprintf(errmsg, MESSAGESIZE, "Failed to closedir: %s", work_node.path);
            ////                    errsend(1, errmsg);
            ////                }
            ////#ifdef PLFS
            ////            }
            ////#endif


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

            // did the readdir() loop exit because of an error?
            if (! readdir_p) {
                errsend_fmt(NONFATAL, "readdir (entry %d) failed on %s (%s)\n",
                        buffer_count, work_node.path, p_work->strerror());
            }

            // done with 
            if (! p_work->closedir()) {
                errsend_fmt(NONFATAL, "Failed to open (%s) dir %s\n", 
                        p_work->class_name().get(), p_work->path());
            }
        }



        //we were provided a file list
        //
        // NOTE: We'll just assume the file-list is stored on a POSIX
        //       filesys, so we don't have to add fgets() methods to all
        //       the PATH subclasses.
        else {
            fp = fopen(work_node.path, "r");
            while (fgets(work_node.path, PATHSIZE_PLUS, fp) != NULL) {
                size_t path_len = strlen(work_node.path);
                if (work_node.path[path_len -1] == '\n') {
                    work_node.path[path_len -1] = '\0';
                }
                workbuffer[buffer_count] = work_node;
                buffer_count++;
                if (buffer_count != 0 && buffer_count % STATBUFFER == 0) {
                    process_stat_buffer(workbuffer, &buffer_count, base_path, dest_node, o, rank);
                }
            }
            fclose(fp);
        }
    }
    while(buffer_count != 0) {
        // process any remaining partially-filled workbuffer contents
        while(buffer_count != 0) {
            process_stat_buffer(workbuffer, &buffer_count, base_path, dest_node, o, rank);
        }
        free(workbuf);
        send_manager_work_done(rank);
    }
}


/**
 * This function tests the metadata of the two nodes
 * to see if they are the same. For files that are chunkable,
 * it looks to see if CTM (chunk transfer metadata) exists for 
 * the source file. If it does, then it assumes that there was 
 * an aborted transfer, and the files are NOT the same!
 *
 * @param src        a path_item structure containing
 *           the metadata for the souce file
 * @param dst        a path_item structure containing
 *           the metadata for the destination
 *           file
 * @param o      the PFTOOL global options structure
 *
 * @return 1 (TRUE) if the metadata matches and no CTM
 *   exists for a chunkable file. 0 (FALSE) otherwise.
 */
int samefile(path_item src, path_item dst, struct options o) {
    int rc = 0;                         // return code of function

    rc = (src.st.st_size == dst.st.st_size &&           // compare metadata - check size, mtime, mode, and owners
            (src.st.st_mtime == dst.st.st_mtime  || S_ISLNK(src.st.st_mode)) &&
            src.st.st_mode == dst.st.st_mode &&
            src.st.st_uid == dst.st.st_uid &&
            src.st.st_gid == dst.st.st_gid);
    if (rc && src.st.st_size >= o.chunk_at)             // a chunkable file that looks the same - does it have a CTM?
        rc = !(hasCTM(src.path));                 // if CTM exists -> two file are NOT the same
    return(rc);
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
/**
 * This routine processes a buffer of path_items that represent files
 * that have been stated, and are ready to put on the work list.
 *
 * @param *path_buffer   the buffer to process
 * @param *stat_count    the number of path_items (files) in
 *           the given path_buffer
 * @param base_path  the base or parent directory of the
 *           files being processed
 * @param dest_node  a path_item structure that is a template
 *           for the destination of the transfer
 * @param o      the PFTOOL global options structure
 * @param rank       the process MPI rank of the process
 *           doing the buffer processing
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
    int         num_examined_dirs = 0;
    char        errmsg[MESSAGESIZE];
    char        statrecord[MESSAGESIZE];
    ////    path_item   work_node;
    path_item   out_node;
    int         process = 0;
    int         parallel_dest = 0;
    int dest_exists = FALSE;                    // flag to indicate that the destination for file already exists.

    //stat
    ////    struct stat st;
    struct tm   sttm;
    char        modebuf[15];

    char        timebuf[30];
    int         rc;
    int         i;

    //chunks
    //place_holder fo current chunk_size
    size_t      chunk_size = 0;
    size_t      chunk_at = 0;
    size_t      num_bytes_seen = 0;

    //500 MB
    size_t      ship_off = 524288000;

    //int chunk_size = 1024;
    off_t       chunk_curr_offset = 0;
    int         idx = 0;

    //classification
    path_item   dirbuffer[DIRBUFFER];
    int         dir_buffer_count = 0;

    path_item   regbuffer[COPYBUFFER];
    int         reg_buffer_count = 0;

#ifdef TAPE
    path_item   tapebuffer[TAPEBUFFER];
    int         tape_buffer_count = 0;
    int         num_examined_tapes = 0;
    size_t      num_examined_tape_bytes = 0;
#endif

#ifdef FUSE_CHUNKER
    struct timeval tv;
    char        myhost[512];
    char        fusepath[PATHSIZE_PLUS];
    int         fuse_num;
    int         fuse_fd;
    char        linkname[PATHSIZE_PLUS];
    int         numchars;
#endif


    //write_count = stat_count;
    writesize = MESSAGESIZE * MESSAGEBUFFER;
    writebuf = (char *) malloc(writesize * sizeof(char));

    out_position = 0;
    for (i = 0; i < *stat_count; i++) {

        process = 0;

        ////        work_node = path_buffer[i];
        ////        st = work_node.st;
        ////
        ////        // NOTE: This is no longer a good way to check whether two files
        ////        //       are the same.  The small problem is that this just assumes
        ////        //       that two POSIX files on different filesystems would never
        ////        //       have the same inode-number.  It's pretty unlikely that
        ////        //       they ever would, but not impossible.  The bigger problem
        ////        //       is that if these are both on object storage systems, it's
        ////        //       *guaranteed* that they will have the same inode-number,
        ////        //       because object filesystems dont have inodes, so st.st_ino
        ////        //       is always zero.
        ////
        ////        //if the source is the initial destination
        ////        if (st.st_ino == dest_node->st.st_ino) {
        ////            write_count--;
        ////            continue;
        ////        }
        PRINT_IO_DEBUG("rank %d: process_stat_buffer() processing entry %d: %s\n", rank, i, work_node.path);
        process = FALSE;

        path_item&  work_node = path_buffer[i]; // avoid a copy
        PathPtr p_work(PathFactory::create_shallow(&path_buffer[i]));
        PathPtr p_dest(PathFactory::create_shallow(dest_node));
        PathPtr p_out;


        // TBD: This should test for *identical* items (e.g. same POSIX
        //      inode num)
        if (p_work->identical(p_dest)) {
            write_count--;
            continue;
        }

        //check if the work is a directory
        ////        else if (S_ISDIR(st.st_mode))
        else if (p_work->is_dir())
        {
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
            memset(&out_node, 0, sizeof(path_item) - PATHSIZE_PLUS +1);
            get_output_path(out_node.path, base_path, &work_node, dest_node, o);

            ////            rc = stat_item(&out_node, o);
            p_out = PathFactory::create_shallow(&out_node);
            p_out->stat();

            if (o.work_type == COPYWORK) {
                process = 1;

                ////#ifdef PLFS
                ////                if(out_node.ftype == PLFSFILE) {
                ////                    parallel_dest = 1;
                ////                    work_node.dest_ftype = PLFSFILE;
                ////                }
                ////                else {
                ////                    parallel_dest = o.parallel_dest;
                ////                }
                ////#endif
                p_work->dest_ftype(p_out->node().ftype); // (matches the intent of old code, above?)
                if (p_out->supports_n_to_1())
                    parallel_dest = 1;

                //if the out path exists
                ////                if (rc == 0)
                if (p_out->exists()) {

                    if (o.different == 1) {
                        // user only wants to operate on source-files that
                        // are "different" from the corresponding
                        // dest-files.


                        //check size, mtime, mode, and owners
                        //
                        // NOTE: S3 objects DO NOT HAVE create-time or
                        //       access-time.  Therefore, their metadata is
                        //       initialized with ctime=mtime, and
                        //       atime=mtime.  Therefore, if you compare a
                        //       POSIX file having values for ctime, atime,
                        //       and mtime that are not all the same, with
                        //       any S3 object, the two sets of metadata
                        //       will *always* differ in these values, even
                        //       if you freshly create them and set all
                        //       these values to match the original.
                        //       Therefore, it might make sense to
                        //       reconsider this test.
                        if (work_node.st.st_size == out_node.st.st_size &&
                                (work_node.st.st_mtime == out_node.st.st_mtime  ||
                                 S_ISLNK(work_node.st.st_mode)) &&
                                work_node.st.st_mode == out_node.st.st_mode &&
                                work_node.st.st_uid == out_node.st.st_uid &&
                                work_node.st.st_gid == out_node.st.st_gid) {

                            process = 0; // source/dest are the same, so skip
                        }
                    }



                    if (process == 1) {

#ifdef FUSE_CHUNKER
                        if (out_node.ftype == FUSEFILE) {

                            //it's a fuse file: delete the link-dest, and the link itself
                            if (o.different == 0 ||
                                    (o.different == 1 && out_node.st.st_size > work_node.st.st_size)) {

                                // <linkname> = name of the link-destination
                                numchars = readlink(out_node.path, linkname, PATHSIZE_PLUS);
                                if (numchars < 0) {
                                    snprintf(errmsg, MESSAGESIZE, "Failed to read link %s", out_node.path);
                                    errsend(FATAL, errmsg);
                                }
                                linkname[numchars] = '\0';

                                //first unlink the actual fuse file
                                rc = unlink(linkname);
                                if (rc < 0) {
                                    snprintf(errmsg, MESSAGESIZE, "Failed to unlink (1) %s -- ftype==%d",
                                            linkname, out_node.ftype);
                                    errsend(FATAL, errmsg);
                                }
                                //now unlink the symlink
                                rc = unlink(out_node.path);
                                if (rc != 0) {
                                    sprintf(errmsg, "Failed to unlink file %s", out_node.path);
                                    errsend(NONFATAL, errmsg);
                                }

                                p_out.reset(); // p_out was created shallow, and we're going to change ftype
                                out_node.ftype = NONE;
                            }
                        }
                        else {
#endif

                            //it's not fuse, unlink
                            ////#ifdef PLFS
                            ////                            if (out_node.ftype == PLFSFILE){
                            ////                                rc = plfs_unlink(out_node.path);
                            ////                            }
                            ////                            else{
                            ////#endif
                            ////                                rc = unlink(out_node.path);
                            ////#ifdef PLFS
                            ////                            }
                            ////#endif
                            ////
                            ////                            if (rc < 0) {
                            ////                                snprintf(errmsg, MESSAGESIZE, "Failed to unlink (2) %s -- ftype==%d", out_node.path, out_node.ftype);
                            ////                                errsend(FATAL, errmsg);
                            ////                            }
                            ////
#if 0
                            // COMMENTED OUT.  See NOTE, below.
                            if (! p_out->unlink()) {
                                errsend_fmt(FATAL, "Failed to unlink (2) %s: %s\n",
                                        p_out->path(), p_out->strerror());
                            }
#else
                            // NOTE: the old code considered
                            //       (return-code<0) to be an error.  But
                            //       plfs_unlink() and unlink() both always
                            //       return >= 0.  (Errors are small
                            //       positive integers.)  Therefore, the
                            //       old code never actually tested for
                            //       errors, and, if I do so now, I
                            //       discover that plfs_unlink() is
                            //       returning ENOENT when unlinking, even
                            //       though the unlink is apparently
                            //       successful.  Therefore, I'm stifling
                            //       this test.
                            p_out->unlink();
#endif

                            p_out.reset(); // p_out was created shallow, and we're going to change ftype
                            out_node.ftype = NONE;

#ifdef FUSE_CHUNKER
                        }
#endif
                    }
                }
                else {
                    p_out.reset(); // p_out was created shallow, and we're going to change ftype
                    out_node.ftype = NONE;
                }
            } // end COPYWORK
            else if (o.work_type == COMPAREWORK) {      // preping for COMPAREWORK, which means we simply assign the destination type to the source file info
                process = 1;
                work_node.dest_ftype = out_node.ftype;
            }



            if (process == 1) {

                //parallel filesystem can do n-to-1
                if (parallel_dest) {
                    CTM *ctm = (CTM *)NULL;             // CTM structure used with chunked files   

#ifdef FUSE_CHUNKER
                    //non_archive files need to not be
                    // fuse. (i.e. dest_ftype==FUSEFILE is only for
                    // out_nodes where o.archive_path is the first part of
                    // the path)
                    if(strncmp((o.archive_path, out_node.path, strlen(o.archive_path)) != 0)
                            && (work_node.dest_ftype == FUSEFILE)) {

                        work_node.dest_ftype = REGULARFILE;
                    }
#endif

                    chunk_size = o.chunksize;
                    chunk_at = o.chunk_at;



#ifdef FUSE_CHUNKER
                    if(work_node.dest_ftype == FUSEFILE) {
                        chunk_size = o.fuse_chunksize;
                        chunk_at = o.fuse_chunk_at;

                    }
                    else if (work_node.ftype == FUSEFILE) {
                        set_fuse_chunk_data(&work_node);
                        chunk_size = work_node.length;
                    }
                    if (work_node.dest_ftype == FUSEFILE) {
                        if (o.work_type == COPYWORK) {
                            if (out_node.ftype == NONE) {
                                gettimeofday(&tv, NULL);
                                srand(tv.tv_sec);
                                gethostname(myhost, sizeof(myhost));
                                fuse_num = (int) rand() % o.fuse_chunkdirs;
                                sprintf(fusepath, "%s/%08d.DIR/%s.%d.%d.%zd.REG",
                                        o.fuse_path,
                                        fuse_num,
                                        myhost,
                                        (int) tv.tv_sec,
                                        (int) tv.tv_usec,
                                        chunk_size);
                                fuse_fd = open(fusepath, O_CREAT | O_RDWR);
                                close(fuse_fd);
                                symlink(fusepath, out_node.path);
                            }
                        }
                    }
                }
            }
#endif



#ifdef PLFS
            if (work_node.desttype == PLFSFILE) {
                chunk_size = o.plfs_chunksize;
                chunk_at = 0;
            }
#endif
            if (work_node.st.st_size == 0) {        // handle zero-length source file
                work_node.chkidx = 0;
                work_node.chksz = 0;
                regbuffer[reg_buffer_count] = work_node;
                reg_buffer_count++;
            }

            if (work_node.st.st_size >= chunk_at) {     // working with a chunkable file
                int ctmExists = hasCTM(work_node.path);

                if (o.different && ctmExists && dest_exists) {// we are doing a conditional transfer & CTM exists -> populate CTM structure
                    ctm = getCTM(work_node.path,((long)ceil(work_node.st.st_size/((double)chunk_size))),chunk_size);
                    if(IO_DEBUG_ON) {
                        char ctm_flags[2048];
                        char *ctmstr = ctm_flags;
                        int ctmlen = 2048;

                        PRINT_IO_DEBUG("rank %d: process_stat_buffer() Reading persistenr stor of CTM: %s\n", rank, tostringCTM(ctm,&ctmstr,&ctmlen));
                    }
                }
                else if (ctmExists)               // get rid of the CTM on the file if we are NOT doing a conditional transfer
                    purgeCTM(work_node.path);   
            }
            chunk_curr_offset = 0;              // keeps track of current offset in file for chunk.
            idx = 0;                    // keeps track of the chunk index
            while (chunk_curr_offset < work_node.st.st_size) {
                work_node.chkidx = idx;         // assign the chunk index
                // non-chunked file or file is a link or metadata compare work - just send the whole file
                if (work_node.st.st_size < chunk_at || work_node.ftype == LINKFILE || (o.work_type == COMPAREWORK && o.meta_data_only)) {
                    work_node.chksz = work_node.st.st_size;   // set chunk size to size of file
                    chunk_curr_offset = work_node.st.st_size; // set chunk offset to end of file
                    PRINT_IO_DEBUG("rank %d: process_stat_buffer() non-chunkable file   chunk index: %d   chunk size: %ld\n", rank, work_node.chkidx, work_node.chksz);
                }
                else {                  // having to chunk the file
                    work_node.chksz = (ctm)?ctm->chnksz:chunk_size;
                    chunk_curr_offset += (((chunk_curr_offset + work_node.chksz) >  work_node.st.st_size)?(work_node.st.st_size-chunk_curr_offset):work_node.chksz);
                    idx++;
                }
#ifdef TAPE
                if (work_node.ftype == MIGRATEFILE
#ifdef FUSE_CHUNKER
                        || (work_node.st.st_size > 0 && work_node.st.st_blocks == 0 && work_node.ftype == FUSEFILE)
#endif
                   ) {
                    tapebuffer[tape_buffer_count] = work_node;
                    tape_buffer_count++;
                    if (tape_buffer_count % TAPEBUFFER == 0) {
                        send_manager_tape_buffer(tapebuffer, &tape_buffer_count);
                    }
                }
                else {
#endif
                    if (!o.different || !chunktransferredCTM(ctm,work_node.chkidx)) { // if a non-conditional transfer or if the chunk did not make on the first one ...
                        num_bytes_seen += work_node.chksz;  // keep track of number of bytes processed
                        regbuffer[reg_buffer_count] = work_node;// copy source file info into sending buffer
                        reg_buffer_count++;
                        PRINT_IO_DEBUG("rank %d: process_stat_buffer() adding chunk index: %d   chunk size: %ld\n", rank, work_node.chkidx, work_node.chksz);
                        if (reg_buffer_count % COPYBUFFER == 0 || num_bytes_seen >= ship_off) {
                            PRINT_MPI_DEBUG("rank %d: process_stat_buffer() parallel destination - sending %d reg buffers to manager.\n", rank, reg_buffer_count);
                            send_manager_regs_buffer(regbuffer, &reg_buffer_count);
                            num_bytes_seen = 0;
                        }
                    } // end send test
#ifdef TAPE
                }
#endif
            } // end file/chunking loop
            if (ctm) freeCTM(&ctm);           // if CTM structure allocated it -> free the memory now
        } // end Parallel destination
        else {                      // non-parallel destination
            work_node.chkidx = 0;           // for non-chunked files, index is always 0
            work_node.chksz = work_node.st.st_size;     // set chunk size to size of file

            num_bytes_seen += work_node.chksz;          // send this off to the manager work list, if ready to
            regbuffer[reg_buffer_count] = work_node;
            reg_buffer_count++;
            if (reg_buffer_count % COPYBUFFER == 0 || num_bytes_seen >= ship_off) {
                PRINT_MPI_DEBUG("rank %d: process_stat_buffer() non-parallel destination - sending %d reg buffers to manager.\n", rank, reg_buffer_count);
                send_manager_regs_buffer(regbuffer, &reg_buffer_count);
                num_bytes_seen = 0;
            }
        }
    }
}


////        if (! S_ISDIR(st.st_mode))
if (! S_ISDIR(work_node.st.st_mode)) {
    num_examined_files++;
    num_examined_bytes += work_node.st.st_size;
#ifdef TAPE
    if (work_node.ftype == MIGRATEFILE) {
        num_examined_tapes++;
        num_examined_tape_bytes += work_node.st.st_size;
    }
#endif
}
printmode(work_node.st.st_mode, modebuf);
memcpy(&sttm, localtime(&work_node.st.st_mtime), sizeof(sttm));
strftime(timebuf, sizeof(timebuf), "%a %b %d %Y %H:%M:%S", &sttm);
//if (work_node.st.st_size > 0 && work_node.st.st_blocks == 0){
if (o.verbose) {
    if (work_node.ftype == MIGRATEFILE) {
        sprintf(statrecord, "INFO  DATASTAT M %s %6lu %6d %6d %21zd %s %s\n",
                modebuf, (long unsigned int) work_node.st.st_blocks,
                work_node.st.st_uid, work_node.st.st_gid,
                (size_t) work_node.st.st_size, timebuf, work_node.path);
    }
    else if (work_node.ftype == PREMIGRATEFILE) {
        sprintf(statrecord, "INFO  DATASTAT P %s %6lu %6d %6d %21zd %s %s\n",
                modebuf, (long unsigned int) work_node.st.st_blocks,
                work_node.st.st_uid, work_node.st.st_gid,
                (size_t) work_node.st.st_size, timebuf, work_node.path);
    }
    else {
        sprintf(statrecord, "INFO  DATASTAT - %s %6lu %6d %6d %21zd %s %s\n",
                modebuf, (long unsigned int) work_node.st.st_blocks,
                work_node.st.st_uid, work_node.st.st_gid,
                (size_t) work_node.st.st_size, timebuf, work_node.path);
    }
    MPI_Pack(statrecord, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);
    write_count++;
    if (write_count % MESSAGEBUFFER == 0) {
        write_buffer_output(writebuf, writesize, write_count);
        out_position = 0;
        write_count = 0;
    }
}
}


//incase we tried to copy a file into itself
if (o.verbose) {
    writesize = MESSAGESIZE * write_count;
    writebuf = (char *) realloc(writebuf, writesize * sizeof(char));
    write_buffer_output(writebuf, writesize, write_count);
}
while(dir_buffer_count != 0) {
    send_manager_dirs_buffer(dirbuffer, &dir_buffer_count);
}
while (reg_buffer_count != 0) {
    send_manager_regs_buffer(regbuffer, &reg_buffer_count);
}
#ifdef TAPE
while (tape_buffer_count != 0) {
    send_manager_tape_buffer(tapebuffer, &tape_buffer_count);
}
send_manager_tape_stats(num_examined_tapes, num_examined_tape_bytes);
#endif
send_manager_examined_stats(num_examined_files, num_examined_bytes, num_examined_dirs);


//free malloc buffers
free(writebuf);
*stat_count = 0;
}



#ifdef TAPE
void worker_taperecall(int rank, int sending_rank, path_item* dest_node, struct options& o) {
    MPI_Status status;
    char *workbuf, *writebuf;
    char recallrecord[MESSAGESIZE];
    int worksize, writesize;
    int position, out_position;
    int read_count;
    int write_count = 0;
    path_item work_node;
    path_item workbuffer[STATBUFFER];
    int buffer_count = 0;
    size_t num_bytes_seen = 0;
    //500 MB
    size_t ship_off = 524288000;
    int i, rc;
    PRINT_MPI_DEBUG("rank %d: worker_taperecall() Receiving the read_count from %d\n", rank, sending_rank);
    if (MPI_Recv(&read_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive read_count\n");
    }
    worksize = read_count * sizeof(path_list);
    workbuf = (char *) malloc(worksize * sizeof(char));
    writesize = MESSAGESIZE * read_count;
    writebuf = (char *) malloc(writesize * sizeof(char));
    //gather the path to stat
    PRINT_MPI_DEBUG("rank %d: worker_taperecall() Receiving the workbuf from %d\n", rank, sending_rank);
    if (MPI_Recv(workbuf, worksize, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive workbuf\n");
    }
    for (i = 0; i < read_count; i++) {
        PRINT_MPI_DEBUG("rank %d: worker_taperecall() unpacking work_node from %d\n", rank, sending_rank);
        MPI_Unpack(workbuf, worksize, &position, &work_node, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);
        rc = work_node.one_byte_read(work_node.path);
        if (rc == 0) {
            workbuffer[buffer_count] = work_node;
            buffer_count += 1;
            if (buffer_count % COPYBUFFER == 0 || num_bytes_seen >= ship_off) {
                send_manager_regs_buffer(workbuffer, &buffer_count);
            }
            if (o.verbose) {
                sprintf(recallrecord, "INFO  DATARECALL Recalled file %s offs %ld len %ld\n", work_node.path, work_node.chkidx, work_node.chksz);
                MPI_Pack(recallrecord, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);
                write_count++;
                if (write_count % MESSAGEBUFFER == 0) {
                    write_buffer_output(writebuf, writesize, write_count);
                    out_position = 0;
                    write_count = 0;
                }
            }
        }
    }
    if (o.verbose) {
        writesize = MESSAGESIZE * write_count;
        writebuf = (char *) realloc(writebuf, writesize * sizeof(char));
        write_buffer_output(writebuf, writesize, write_count);
    }
    while (buffer_count != 0) {
        send_manager_regs_buffer(workbuffer, &buffer_count);
    }
    send_manager_work_done(rank);
    free(workbuf);
    free(writebuf);
}
#endif

//When a worker is told to copy, it comes here
void worker_copylist(int             rank,
        int             sending_rank,
        const char*     base_path,
        path_item*      dest_node,
        struct options& o) {

    MPI_Status     status;
    char*          workbuf;
    char*          writebuf;
    SyndataBufPtr  synbuf = NULL;
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

#ifdef FUSE_CHUNKER
    //partial file restart
    struct utimbuf ut;
    struct utimbuf chunk_ut;

    uid_t          userid;
    uid_t          chunk_userid;

    gid_t          groupid;
    gid_t          chunk_groupid;
#endif

    PRINT_MPI_DEBUG("rank %d: worker_copylist() Receiving the read_count from %d\n", rank, sending_rank);
    if (MPI_Recv(&read_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive read_count\n");
    }
    worksize = read_count * sizeof(path_list);
    workbuf = (char *) malloc(worksize * sizeof(char));
    writesize = MESSAGESIZE * read_count;
    writebuf = (char *) malloc(writesize * sizeof(char));

    //gather the path to stat
    PRINT_MPI_DEBUG("rank %d: worker_copylist() Receiving the workbuf from %d\n", rank, sending_rank);
    if (MPI_Recv(workbuf, worksize, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive workbuf\n");
    }

#ifdef GEN_SYNDATA
    if(o.syn_size) {
        // If no pattern id is given -> use rank as a seed for random data
        synbuf = syndataCreateBufferWithSize((o.syn_pattern[0] ? o.syn_pattern : NULL),
                (o.syn_size >= 0) ? o.syn_size : -rank));
        if (! synbuf)
            errsend_fmt(FATAL, "Rank %d: Failed to allocate synthetic-data buffer\n", rank);
    }
#endif

    position = 0;
    out_position = 0;
    for (i = 0; i < read_count; i++) {

        PRINT_MPI_DEBUG("rank %d: worker_copylist() unpacking work_node from %d\n", rank, sending_rank);
        MPI_Unpack(workbuf, worksize, &position, &work_node, sizeof(path_item), MPI_CHAR, MPI_COMM_WORLD);
        offset = work_node.chkidx*work_node.chksz;
        length = ((offset+work_node.chksz)>work_node.st.st_size)?(work_node.st.st_size-offset):work_node.chksz;
        PRINT_MPI_DEBUG("rank %d: worker_copylist() chunk index %d unpacked. offset = %ld   length = %ld\n", rank, work_node.chkidx, offset, length);
        get_output_path(out_node.path, base_path, &work_node, dest_node, o);
        out_node.fstype = o.dest_fstype;
#ifdef FUSE_CHUNKER
        if (work_node.dest_ftype != FUSEFILE) {
#endif
            rc = copy_file(&work_node, &out_node, o.blocksize, rank, synbuf);

#ifdef FUSE_CHUNKER
        }
        else {
            userid = work_node.st.st_uid;
            groupid = work_node.st.st_gid;
            ut.actime = work_node.st.st_atime;
            ut.modtime = work_node.st.st_mtime;
            rc = get_fuse_chunk_attr(out_node.path, offset, length, &chunk_ut, &chunk_userid, &chunk_groupid);
            if ( rc == -1 ||
                    chunk_userid != userid ||
                    chunk_groupid != groupid ||
                    chunk_ut.actime != ut.actime||
                    chunk_ut.modtime != ut.modtime) { //not a match

                rc = copy_file(&work_node, &out_node, o.blocksize, rank, synbuf);
                set_fuse_chunk_attr(out_node.path, offset, length, ut, userid, groupid);
            }
        }
#endif

        if (rc >= 0) {
            if (o.verbose) {
                if (S_ISLNK(work_node.st.st_mode)) {
                    sprintf(copymsg, "INFO  DATACOPY Created symlink %s from %s\n", out_node.path, work_node.path);
                }
                else {
                    sprintf(copymsg, "INFO  DATACOPY Copied %s offs %lld len %lld to %s\n", work_node.path, (long long)offset, (long long)length, out_node.path);
                }
                //MPI_Pack(copymsg, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);
                //write_buffer_output(copymsg, MESSAGESIZE, 1);
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
    /*if (o.verbose) {
      write_buffer_output(writebuf, writesize, read_count);
      }*/
    //update the chunk information
    if (buffer_count > 0) {
        send_manager_chunk_busy();
        update_chunk(chunks_copied, &buffer_count);
    }
    if (num_copied_files > 0 || num_copied_bytes > 0) {
        send_manager_copy_stats(num_copied_files, num_copied_bytes);
    }
    send_manager_work_done(rank);
#ifdef GEN_SYNDATA
    syndataDestroyBuffer(synbuf);
#endif
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

    PRINT_MPI_DEBUG("rank %d: worker_copylist() Receiving the read_count from %d\n", rank, sending_rank);
    if (MPI_Recv(&read_count, 1, MPI_INT, sending_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
        errsend(FATAL, "Failed to receive read_count\n");
    }
    worksize = read_count * sizeof(path_list);
    workbuf = (char *) malloc(worksize * sizeof(char));
    writesize = MESSAGESIZE * read_count;
    writebuf = (char *) malloc(writesize * sizeof(char));
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
        get_output_path(out_node.path, base_path, &work_node, dest_node, o);
        stat_item(&out_node, o);
        //sprintf(copymsg, "INFO  DATACOPY Copied %s offs %lld len %lld to %s\n", slavecopy.req, (long long) slavecopy.offset, (long long) slavecopy.length, copyoutpath)
        offset = work_node.chkidx*work_node.chksz;
        length = work_node.chksz;
        rc = compare_file(&work_node, &out_node, o.blocksize, o.meta_data_only);
        if (o.meta_data_only || work_node.ftype == LINKFILE) {
            sprintf(copymsg, "INFO  DATACOMPARE compared %s to %s", work_node.path, out_node.path);
        }
        else {
            sprintf(copymsg, "INFO  DATACOMPARE compared %s offs %lld len %lld to %s", work_node.path, (long long)offset, (long long)length, out_node.path);
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
        if (o.verbose) {
            MPI_Pack(copymsg, MESSAGESIZE, MPI_CHAR, writebuf, writesize, &out_position, MPI_COMM_WORLD);
        }
        //file is not 'chunked'
        if (offset == 0 && length == work_node.st.st_size) {
            num_compared_files +=1;
            num_compared_bytes += length;
        }
        else {
            chunks_copied[buffer_count] = work_node;
            buffer_count++;
        }
    }
    if (o.verbose) {
        write_buffer_output(writebuf, writesize, read_count);
    }
    //update the chunk information
    if (buffer_count > 0) {
        send_manager_chunk_busy();
        update_chunk(chunks_copied, &buffer_count);
    }
    //for all non-chunked files
    if (num_compared_files > 0 || num_compared_bytes > 0) {
        send_manager_copy_stats(num_compared_files, num_compared_bytes);
    }
    send_manager_work_done(rank);
    free(workbuf);
    free(writebuf);
}



