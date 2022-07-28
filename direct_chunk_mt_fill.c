/* direct_chunk_mt_fill.c
 *
 * Sample program for ITER demonstrating direct chunk operations
 *
 * This version uses a background thread to directly write data to the
 * dataset
 *
 * To build:
 *      h5cc -o writer direct_chunk_mt_fill.c -lpthread
 *
 * - Does NOT require the thread-safe library
 * - DOES require POSIX-y things (sorry Windows users)
 *
 * To run:
 *      - Run the program
 *      - It will generate one 10-integer chunk per second
 *      - ctrl-c stops the program
 */

#include <hdf5.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

/* Some global constants */

volatile sig_atomic_t stop;

const char *FILE_NAME = "direct_chunk_mt_fill.h5";
const char *DSET_NAME = "data";

#define RANK 1

/* SO SMALL - Don't make chunks this size in real code! */
const hsize_t CHUNK_SIZE = 10;

const int FILL_VALUE = -1;

#define SUCCEED   0
#define FAIL    (-1)

void
ctrl_c_handler(int signum)
{
    (void)signum;

    stop = 1;
}


herr_t
setup(void)
{
    hid_t fapl_id = H5I_INVALID_HID;
    hid_t fid     = H5I_INVALID_HID;
    hid_t sid     = H5I_INVALID_HID;
    hid_t dcpl_id = H5I_INVALID_HID;
    hid_t did     = H5I_INVALID_HID;

    hsize_t current_dims[RANK] = {0};
    hsize_t max_dims[RANK]     = {H5S_UNLIMITED};
    hsize_t chunk_dims[RANK]   = {CHUNK_SIZE};

    /* fapl */
    if ((fapl_id = H5Pcreate(H5P_FILE_ACCESS)) == H5I_INVALID_HID)
        goto badness;
    if (H5Pset_libver_bounds(fapl_id, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST))
        goto badness;

    /* Create file */
    if ((fid = H5Fcreate(FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id)) == H5I_INVALID_HID)
        goto badness;

    /* Dataspace for dataset */
    if ((sid = H5Screate_simple(RANK, current_dims, max_dims)) == H5I_INVALID_HID)
        goto badness;

    /* dcpl */
    if ((dcpl_id = H5Pcreate(H5P_DATASET_CREATE)) == H5I_INVALID_HID)
        goto badness;
    if (H5Pset_chunk(dcpl_id, RANK, chunk_dims) < 0)
        goto badness;
    if (H5Pset_fill_value(dcpl_id, H5T_NATIVE_INT, &FILL_VALUE) < 0)
        goto badness;

    /* Create dataset */
    if ((did = H5Dcreate2(fid, DSET_NAME, H5T_NATIVE_INT, sid, H5P_DEFAULT, dcpl_id, H5P_DEFAULT)) == H5I_INVALID_HID)
        goto badness;

    /* Shutdown */
    if (H5Pclose(fapl_id) < 0)
        goto badness;
    if (H5Fclose(fid) < 0)
        goto badness;
    if (H5Sclose(sid) < 0)
        goto badness;
    if (H5Pclose(dcpl_id) < 0)
        goto badness;
    if (H5Dclose(did) < 0)
        goto badness;

    return SUCCEED;

badness:

    H5E_BEGIN_TRY
    {
        H5Pclose(fapl_id);
        H5Fclose(fid);
        H5Sclose(sid);
        H5Pclose(dcpl_id);
        H5Dclose(did);
    }
    H5E_END_TRY;

    return FAIL;
}

herr_t
extend_dataset(hid_t did, hsize_t size)
{
    hsize_t new_dims[RANK] = {size};

    if (H5Dset_extent(did, new_dims) < 0)
        goto badness;

    return SUCCEED;

badness:

    return FAIL;
}

herr_t
direct_write(hid_t did, hsize_t offset)
{
    int     *buf = NULL;
    size_t   buf_size;
    uint32_t filter_mask = 0; /* We're not skipping any filters */
    int      value;           /* The data value we're writing to the buffer */

    buf_size = CHUNK_SIZE * sizeof(int);

    /* For synthetic data, we just fill the chunk with the chunk number.
     * That should make it easy to spot screwups.
     */
    value = (int)(offset / CHUNK_SIZE);
    if (value > INT_MAX) {
        fprintf(stderr, "can't have more than INT_MAX chunks in this example\n");
        goto badness;
    }
    if (NULL == (buf = malloc(buf_size * sizeof(char))))
        goto badness;
    for (int i = 0; i < CHUNK_SIZE; i++)
        buf[i] = value;

    /* Write the data to the chunk */
    if (H5Dwrite_chunk(did, H5P_DEFAULT, filter_mask, &offset, (size_t)buf_size, (void *)buf) < 0)
        goto badness;

    free(buf);

    return SUCCEED;

badness:
    free(buf);
    return FAIL;
}

int
main(void)
{
    struct sigaction sa;

    /* Catch ctrl-c */
    sa.sa_handler = ctrl_c_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    sigaction(SIGINT, &sa, NULL);

    /* Set up file and dataset */
    if (setup() < 0)
        goto badness;

    printf("FILE CREATION COMPLETE\n");
    printf("PRESS CTRL-C TO HALT DATA GENERATION\n");

    hid_t fid     = H5I_INVALID_HID;
    hid_t did     = H5I_INVALID_HID;

    if ((fid = H5Fopen(FILE_NAME, H5F_ACC_RDWR | H5F_ACC_SWMR_WRITE, H5P_DEFAULT)) == H5I_INVALID_HID)
        goto badness;
    if ((did = H5Dopen2(fid, DSET_NAME, H5P_DEFAULT)) == H5I_INVALID_HID)
        goto badness;

    /* Number of dataset chunks */
    uint64_t n_chunks = 0;

    while (!stop) {

        /* Extend by one chunk
         *
         * WARNING: This is wildly inefficient - don't extend by one small
         *          chunk at a time
         */

        /* The write offset where we'll be scribbling our data */
        hsize_t write_offset = n_chunks * CHUNK_SIZE;

        /* The new size of the dataset after we extend */
        hsize_t new_size = (n_chunks + 1) * CHUNK_SIZE;

        if (extend_dataset(did, new_size) < 0)
            goto badness;

        if (direct_write(did, write_offset) < 0)
            goto badness;

        n_chunks += 1;

        sleep(1);
    }

    if (H5Fclose(fid) < 0)
        goto badness;
    if (H5Dclose(did) < 0)
        goto badness;

    printf("DONE\n");

    return EXIT_SUCCESS;

badness:
    printf("BADNESS\n");

    return EXIT_FAILURE;
}
