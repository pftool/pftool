#include "mpii.h"
#include <stdio.h>

#define SLAVE_STRING "-slave_out_of"
#define MASTER_STRING "-num_proc"

MPI_Comm *worlds;

static int orig_argc;
static char **orig_argv = NULL;

/* For synchronizing with children */
static Mutex mutex1, mutex2;
static Cond cond1, cond2;
static int ninit, go;

void MPII_Get_global_init (void);

static void *call_main (void *world)
{
    /* Copy argv */
    char **argv;
    int i;
    argv = mymalloc ((MPI_Comm) world, orig_argc+1, char *);
    for (i = 0; i < orig_argc; i++)
        argv[i] = orig_argv[i];
    argv[orig_argc] = NULL;

    /* Define MPI_COMM_WORLD */
    set_tsd (MPII_commworld_key, world);

    /* Call user program */
    main (orig_argc, argv);

    return NULL;
}

PUBLIC int MPI_Init (int *argc, char ***argv)
{
   int error;
   char *nproc_s, *who_s;
   int i, nproc;
   Thread tid;
   MPI_Comm world;

   /* Parse arguments */
   if (*argc < 3)
   {
     MPI_Initialized (&i);
     if (i)
       return MPII_Error (NULL, MPII_HAVE_INITIALIZED);
     else
       return MPII_Error (NULL, MPII_INIT_ARG);
   }

   MPII_Set_initialized ();

   who_s   = (*argv)[*argc-2];
   nproc_s = (*argv)[*argc-1];

   nproc = atoi (nproc_s);
   if (nproc <= 0)
      return MPII_Error (NULL, MPII_INIT_ARG);

   if (!strcasecmp (who_s, MASTER_STRING))
   {
      MPII_Member **members;
      MPI_Context context;

      /* Setup argc and argv for slaves */
      (*argv)[*argc-2] = SLAVE_STRING;
      orig_argc = *argc;
      orig_argv = *argv;

      /* Initialize threads package */
      init_threads ();

#     ifdef MPII_TSD
          MPII_Tsd_master_init (thread_id (), nproc);
#     endif

      /* Initialize g2tsd_lib if it is linked in (in which case the dummy
       * initialization routine below will be replaced).
       */
      MPII_Get_global_init ();

      MPII_Context_init ();

      worlds = mymalloc (NULL, nproc, MPI_Comm);

      /* Set up local MPI_COMM_WORLD */
      world = mymalloc (NULL, 1, MPII_Comm);
      world->errhandler = MPI_ERRORS_ARE_FATAL;
      world->context = context = MPII_New_context ();
      world->group = mymalloc (world, 1, MPII_Group);
      world->group->rank = 0;
      world->group->size = nproc;
      world->group->refcnt = 1;
      members = mymalloc (world, nproc, MPII_Member *);
      world->group->members = (void **) members;
      new_tsd (MPII_commworld_key);
      set_tsd (MPII_commworld_key, world);

      /* Define members */
      new_tsd (MPII_me_key);
      for (i = 0; i < nproc; i++)
      {
          members[i] = mymalloc (world, 1, MPII_Member);
          if (error = new_mutex (members[i]->mutex))
              thread_error ("MPI_Init: Failed to create child mutex", error);
          if (error = new_cond (members[i]->cond))
              thread_error ("MPI_Init: Failed to create child condition", error);
          if (MPII_queue_init (&(members[i]->queue)))
              MPII_Malloc_error (world);
      }

      /* Start other threads synchronously, waiting for MPI_Init calls */
      if (error = new_mutex (mutex1))
          thread_error ("MPI_Init: Failed to create first mutex", error);
      if (error = new_cond (cond1))
          thread_error ("MPI_Init: Failed to create first condition", error);
      ninit = 1;
      if (error = new_mutex (mutex2))
          thread_error ("MPI_Init: Failed to create second mutex", error);
      if (error = new_cond (cond2))
          thread_error ("MPI_Init: Failed to create second condition", error);
      go = 0;

      worlds[0] = world;

      for (i = 1; i < nproc; i++)
      {
         /* Make child's MPI_COMM_WORLD */
         world = mymalloc (NULL, 1, MPII_Comm);
         world->errhandler = MPI_ERRORS_ARE_FATAL;
         world->context = context;
         world->group = mymalloc (world, 1, MPII_Group);
         world->group->rank = i;
         world->group->size = nproc;
         world->group->members = (void **) members;

         worlds[i] = world;

         /* Spawn the child */
         if (error = spawn_thread (call_main, (void *) world, tid))
         {
            thread_error ("MPI_Init: Failed to spawn thread", error);
            fprintf (stderr, "MPI_Init: I could only make a %d-process run\n",
                  i);
            fprintf (stderr, "MPI_Init: Bailing out\n");

            /* Tell children about error and exit */
            lock (mutex2);
            go = -1;
            notify_all (cond2);
            unlock (mutex2);
            exit_thread (1);
         }

#        ifdef MPII_TSD
             MPII_Tsd_slave_init (tid);
#        endif
      }

      /* Wait for threads to initialize */
#if 0
      printf ("Master: Obtaining mutex1 to wait for initialization signals\n");
#endif
      lock (mutex1);
      while (ninit < nproc)
      {
#if 0
          printf ("Master: so far, %d out of %d have initialized.\n", ninit,
              nproc);
#endif
          if (error = wait (cond1, mutex1))
              thread_error ("MPI_Init[0]: Failed at waiting for children to start up (ignoring)", error);
      }
      unlock (mutex1);

      /* Tell them to continue */
      ninit = 1;        /* for clean-up signals below */
      lock (mutex2);
      go = 1;
      notify_all (cond2);
      unlock (mutex2);

      /* Wait for clean-up signals */
      lock (mutex1);
      while (ninit < nproc)
          if (error = wait (cond1, mutex1))
              thread_error ("MPI_Init[0]: Failed at waiting for children to initialize (ignoring)", error);
      unlock (mutex1);

      /* Clean up */
      delete_mutex (mutex1);
      delete_cond (cond1);
      delete_mutex (mutex2);
      delete_cond (cond2);
   }
   else if (!strcasecmp (who_s, SLAVE_STRING))
   {
      /* Signal the master that we have started up */
#if 0
      printf ("Slave: Obtaining mutex1 for notification of starting up\n");
#endif
      lock (mutex1);
      ninit++;
      if (error = notify (cond1))
          thread_error ("MPI_Init: Couldn't notify master about starting up",
                        error);
      unlock (mutex1);
#if 0
      printf ("Slave: Notified that we have started up\n");
#endif

      /* Wait for a reply */
      lock (mutex2);
      while (!go)
         if (error = wait (cond2, mutex2))
             thread_error ("MPI_Init: Failed at waiting for master to give error code", error);
      unlock (mutex2);

      if (go < 0)       /* Check for error at master */
         exit_thread (1);

      /* Tell the master it is okay to delete the mutices and conds now */
      lock (mutex1);
      ninit++;
      if (error = notify (cond1))
          thread_error ("MPI_Init: Failed at telling master about initialization", error);
      if (error = unlock (mutex1))
          thread_error ("MPI_Init: Master deleted mutex1 too soon!", error);
   }
   else
      return MPII_Error (NULL, MPII_INIT_ARG);

   /* Set local "me" value */
   world = MPI_COMM_WORLD;
   set_tsd (MPII_me_key,
         ((MPII_Member **) (world->group->members))[world->group->rank]);

   *argc -= 2;
   (*argv)[*argc] = NULL;
}

