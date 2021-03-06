#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>

typedef struct print_buf {
    /**
     * pointer to runtime buffer, might be stack-allocated
     *
     * wrapped_vsnprintf() will ignore this buffer if it is too small, and allocate bigger one
     */
    char *in_ptr;
    size_t in_size;

    /**
     * pointer to formatted string.
     *
     * wrapped_vsnprintf will copy pointer and size from in_ptr if the result fits,
     * or use pointer to memory allocated using malloc(), in which case need_free member will
     * have non-zero value.
     */
    char *out_ptr;
    char out_size;
    int need_free;
} print_buf;

/*
 * Helper function to expose vsnprintf for logging purposes. va_list is a nightmare to
 * handle directly from rust it seems.
 *
 *    int log_message(const char *fmt, ...)
 *    {
 *        va_list ap;
 *        char stack_buf[100];
 *        int rc;
 *
 *        print_buf my_buf;
 *        my_buf.in_ptr = stack_buf;
 *        my_buf.in_size = 100;
 *
 *        va_start(ap, fmt);
 *        rc = wrapped_vsnprintf(&my_buf, fmt, ap);
 *        va_end(ap);
 *        if (rc < 0) {
 *            return rc;
 *        }
 *
 *        printf(my_buf.out_ptr); // my_buf.out_size contains size of the buffer
 *        if (my_buf.need_free) {
 *            free(my_buf.out_ptr);
 *        }
 *    }
 *
 *    ...
 *
 *    log_message("hello, %s\n", "world");
 *
 * Returns negative value in case of the error.
 */
int wrapped_vsnprintf(print_buf *buf, const char *format, va_list ap)
{
    int rc;

    /* first lets try to use the buffer from Rust runtime */
    buf->out_ptr = buf->in_ptr;
    buf->out_size = buf->in_size;
    buf->need_free = 0;
    rc = vsnprintf(buf->out_ptr, buf->out_size, format, ap);
    if (rc < 0) {
        buf->out_ptr = NULL;
        buf->out_size = 0;
        /* something went wrong, propagate the error to caller */
        return rc;
    }
    if (rc < buf->out_size) {
        /* formatted string fits the buffer, return number of symbols */
        return rc;
    }

    /* otherwise we need to allocate dynamic buffer */
    buf->out_size = buf->in_size * 2;
    buf->out_ptr = malloc(buf->out_size);
    if (buf->out_ptr == NULL) {
        buf->out_size = 0;
        /* for some reasone we unable to allocate memory. Unlikely event */
        return -1;
    }
    do {
        rc = vsnprintf(buf->out_ptr, buf->out_size, format, ap);

        if (rc < 0) {
            free(buf->out_ptr);
            buf->out_ptr = NULL;
            buf->out_size = 0;
            return rc;
        }
        /* everything fits into the buffer, set need_free flag and return it */
        if (rc < buf->out_size) {
            buf->need_free = 1;
            return rc;
        }
        /* otherwise increase the buffer and make another iteration */
        {
            buf->out_size = buf->out_size * 2;
            char *new_ptr = realloc(buf->out_ptr, buf->out_size);
            if (new_ptr == NULL) {
                /* failed to allocate bigger chunk. Reset everything and return error */
                free(buf->out_ptr);
                buf->out_ptr = NULL;
                buf->out_size = 0;
                return -1;
            }
            buf->out_ptr = new_ptr;
        }
    } while (1);
}
