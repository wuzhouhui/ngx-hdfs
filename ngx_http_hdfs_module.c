
/* headers from nginx */
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
/* header from hadoop */
#include <hdfs.h>
/* headers from system */
#include <error.h>
#include <stddef.h>
#include <string.h>

/* default namenode of hdfs */
#define NAMENODE_DEL    "default"
#define NAMENODE        "namenode="
#define USERNAME        "username="
#define PORT            "port="
/* the HDFS_PREFIX should be removed from request */
#define HDFS_PREFIX     "/hdfs"
#define BUFSZ           (1 << 12)
#define UNKNOW_FILE     0
#define PLAIN           1
#define HTML            2
#define IMAGE           3

typedef struct {
    ngx_str_t   namenode;   /* namenode of hdfs */
    ngx_str_t   username;   /* username of hdfs */
    ngx_int_t   port;       /* port of hdfs listening */
} ngx_http_hdfs_loc_conf_t;

static char *ngx_http_hdfs(ngx_conf_t *, ngx_command_t *, void *);
static void *ngx_http_hdfs_create_loc_conf(ngx_conf_t *);
static ngx_int_t ngx_http_hdfs_handler(ngx_http_request_t *);
static ngx_int_t ngx_http_hdfs_get_and_head(ngx_http_request_t *);
static u_char *ngx_http_hdfs_get_path(u_char *);
static ngx_int_t ngx_http_hdfs_handle_empty(ngx_http_request_t *);
static ngx_int_t ngx_http_hdfs_rd_chk(ngx_http_request_t *, hdfsFileInfo *,
        ngx_str_t *);

static ngx_command_t ngx_http_hdfs_commands[] = {
    {
        ngx_string("hdfs"),
        NGX_HTTP_LOC_CONF | NGX_CONF_TAKE123,
        ngx_http_hdfs,
        NGX_HTTP_LOC_CONF_OFFSET,
        0,
        NULL,
    },
    ngx_null_command,
};
static ngx_http_module_t ngx_http_hdfs_ctx = {
    NULL,                             /* preconfiguration */
    NULL,                             /* postconfiguration */
    NULL,                             /* creating the main conf */
    NULL,                             /* initializing the main conf */
    NULL,                             /* creating the server conf */
    NULL,                             /* merging it with the main conf */
    ngx_http_hdfs_create_loc_conf,    /* creating the location conf */
    NULL,                             /* mergint it with the server conf */
};

ngx_module_t ngx_http_hdfs_module = {
    NGX_MODULE_V1,
    &ngx_http_hdfs_ctx,       /* module context */
    ngx_http_hdfs_commands,   /* module directives */
    NGX_HTTP_MODULE,          /* module type */
    NULL,                     /* init master */
    NULL,                     /* init module */
    NULL,                     /* init process */
    NULL,                     /* init thread */
    NULL,                     /* exit thread */
    NULL,                     /* exit process */
    NULL,                     /* exit master */
    NGX_MODULE_V1_PADDING,
};

/*
 * create the location configuration
 */
static void *
ngx_http_hdfs_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_hdfs_loc_conf_t  *conf;

    ngx_conf_log_error(NGX_LOG_DEBUG, cf, 0, "create_loc_conf called");
    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_hdfs_loc_conf_t));
    if (!conf) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                "create_loc_conf: ngx_pcalloc failed");
        return(NGX_CONF_ERROR);
    }
    conf->namenode.data = NULL;
    conf->namenode.len  = 0;
    conf->username.data = NULL;
    conf->username.len  = 0;
    conf->port          = 0;

    return(conf);
}

/*
 * the handler of module.
 */
static ngx_int_t 
ngx_http_hdfs_handler(ngx_http_request_t *r)
{
    ngx_int_t   ret;

    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "handler called");
    if (r->method & (NGX_HTTP_GET | NGX_HTTP_HEAD)) {
        if ((ret = ngx_http_hdfs_get_and_head(r)) != NGX_OK)
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                    "handler: get_and_head failed");
    } else {
        ret = NGX_HTTP_NOT_IMPLEMENTED;
        ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "handler: "
                "method not allowed, r->method = %d", r->method);
    }

    return(ret);
}

/*
 * process http get and head methods.
 */
static ngx_int_t
ngx_http_hdfs_get_and_head(ngx_http_request_t *r)
{
    u_char          *path   = NULL;
    hdfsFS          fs      = NULL;
    hdfsFile        file    = NULL;
    ngx_buf_t       *b      = NULL;
    ngx_int_t       num     = 0, i, read_sofar;
    char            buf[BUFSZ];
    hdfsFileInfo    *file_info = NULL;
    ngx_int_t       ret = NGX_OK, n;
    ngx_chain_t     out, *head = 0, *tmp = 0;
    struct hdfsBuilder          *bld = 0;
    ngx_http_hdfs_loc_conf_t    *conf = NULL;

    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "get_and_head called");
    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "the uri is %s",
            r->uri.data);

    /* we don't need request body */
    if (ngx_http_discard_request_body(r) != NGX_OK)
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                "get: discard_request_body failed");

    if (!(path = ngx_http_hdfs_get_path(r->uri.data))) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                "get_and_head: get_path failed");
        return(NGX_ERROR);
    }

    /* connect to hdfs and get file meta data */
    conf = ngx_http_get_module_loc_conf(r, ngx_http_hdfs_module);
    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0,
            "get_and_head: namenode.data = %s, username = %s, port = %d",
        (conf->namenode.data ? conf->namenode.data : (u_char *)""),
        (conf->username.data ? conf->username.data : (u_char *)""),
        conf->port);
    if (!(bld = hdfsNewBuilder())) {
        ret = NGX_HTTP_INTERNAL_SERVER_ERROR;
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "get_and_head: "
                "hdfsNewBuilder failed, %s", strerror(errno));
        goto clean;
    }
    hdfsBuilderSetNameNode(bld, (char *)conf->namenode.data);
    if (conf->username.data)
        hdfsBuilderSetUserName(bld, (char *)conf->username.data);
    if (conf->port)
        hdfsBuilderSetNameNodePort(bld, conf->port);
    if (!(fs = hdfsBuilderConnect(bld))) {
        /* 
         * the hdfsBuilderConnect frees hdfsBuilder, no matter
         * fails or success.
         */ 
        ret = NGX_HTTP_INTERNAL_SERVER_ERROR;
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "get_and_head: "
                "hdfsBuilderConnect failed, %s", strerror(errno));
        goto clean;
    }
    if (!(file_info = hdfsGetPathInfo(fs, (char *)path))) {
        if (errno != ENOENT) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                    "get_and_head: hdfsGetPathInfo failed, %s, path = %s",
                    strerror(errno), path);
            ret = NGX_ERROR;
        } else {
            ret = NGX_HTTP_NOT_FOUND;
        }
        goto clean;
    }

    num = 1;    /* the number of enties in file_info[] */

    /* readable permission check */
    if (!ngx_http_hdfs_rd_chk(r, file_info, &conf->username)) {
        ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0,
                "get_and_head: user %s has no rights to read %s",
                (conf->username.data ? conf->username.data : (u_char *)""),
                file_info->mName);
        ret = NGX_HTTP_FORBIDDEN;
        goto clean;
    }

    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, 
            "get_and_head: file_info->mName = %s", file_info->mName);
    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "get_and_head:"
        "file_info->mSize = %d", file_info->mSize);

    /* constructing responce header according the type of file */
    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "get_and_head: "
            "file_info->mName = %s", file_info->mName);
    if ((ret = ngx_http_set_content_type(r)) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "get_and_head: "
                "set_content_type failed");
        goto clean;
    }
    r->headers_out.status   = NGX_HTTP_OK;
    r->headers_out.content_length_n = file_info->mSize;

    /* if the request is HEAD, just send the header */
    if (r->method == NGX_HTTP_HEAD) {
        if ((ret = ngx_http_send_header(r)) != NGX_OK)
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                "get_and_head: ngx_http_send_header failed");
        goto clean;
    }

    /* the file(dir) is a file */
    if (file_info->mKind == kObjectKindFile) {
        if (!(file = hdfsOpenFile(fs, (char *)path, O_RDONLY, 0, 0, 0))) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "get_and_head: "
                    "hdfsOpenFile failed, %s", strerror(errno));
            ret = NGX_ERROR;
            goto clean;
        }

        /* send header of response */
        if ((ret = ngx_http_send_header(r)) != NGX_OK) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                "get_and_head: ngx_http_send_header failed");
            goto clean;
        }

        /* send responce body chunk by chunk */
        read_sofar = 0;
        while ((n = hdfsRead(fs, file, buf, sizeof(buf))) > 0) {
            if (!(b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t)))) {
                ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                        "get_and_head: ngx_pcalloc failed");
                ret = NGX_HTTP_INSUFFICIENT_STORAGE;
                goto clean;
            }

            read_sofar  += n;
            b->pos      = (u_char *)buf;
            b->last     = b->pos + n;
            b->memory   = 1;
            b->last_buf = (read_sofar >= r->headers_out.content_length_n);
            out.buf     = b;
            out.next    = NULL;

            /* send the body */
            if ((ret = ngx_http_output_filter(r, &out)) != NGX_OK) {
                ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                        "get_and_head: output_filter failed");
                goto clean;
            }
        }
        if (n < 0) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "get_and_head: "
                    "hdfsRead failed, %s", strerror(errno));
            ret = NGX_ERROR;
            goto clean;
        }
        if (!r->headers_out.content_length_n) { /* the file is empty */
            if ((ret = ngx_http_hdfs_handle_empty(r)) != NGX_OK)
                ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                        "get_and_head: handle_empty failed");
        }
    } else if (file_info->mKind == kObjectKindDirectory) {
        hdfsFreeFileInfo(file_info, num);
        if (!(file_info = hdfsListDirectory(fs, (char *)path, &num))) {
            if (num) {
                ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                        "get_and_head: hdfsListDirectory failed, %s",
                        strerror(errno));
                ret = NGX_ERROR;
                goto clean;
            }

            /* the directory is empty */
            r->headers_out.content_length_n = 0;
            if ((ret = ngx_http_send_header(r)) != NGX_OK) {
                ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                        "get_and_head: send_header failed");
                goto clean;
            }
            if ((ret = ngx_http_hdfs_handle_empty(r)) != NGX_OK)
                ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                        "get_and_head: handle_empty failed");
            goto clean;
        }
        for (head = NULL, i = 0; i < num; i++) {
            if ((b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t))) == NULL) {
                ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                        "get_and_head: ngx_pcalloc failed");
                continue;
            }
            n = strlen(file_info[i].mName);
            b->pos  = (u_char *)file_info[i].mName;
            b->last = (u_char *)(file_info[i].mName + n + 1);

            /* change '\0' to '\n' for beauty, so plus 1 */
            file_info[i].mName[n] = '\n';
            r->headers_out.content_length_n += n + 1;
            b->memory   = 1;
            b->last_buf = 0;

            /* put buffer into the head of the chain */
            if (!(tmp = ngx_pcalloc(r->pool, sizeof(ngx_chain_t)))) {
                ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                        "get_and_head: ngx_pcalloc failed");
                ret = NGX_ERROR;
                goto clean;
            }
            tmp->buf = b;
            if (head == NULL)
                b->last_buf = 1;
            tmp->next = head;
            head = tmp;
        }
        if ((ret = ngx_http_send_header(r)) != NGX_OK) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                "get_and_head: ngx_http_send_header failed");
            goto clean;
        }
        if ((ret = ngx_http_output_filter(r, head)) != NGX_OK)
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                    "get_and_head: output_filter failed");
    } else {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "get_and_head: "
                "unknow mKine: file_info->mKind = %d\n", file_info->mKind);
        ret = NGX_ERROR;
        goto clean;
    }

clean:
    if (file)
        if (hdfsCloseFile(fs, file))
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                    "get_and_head: hdfsCloseFile failed, %s", strerror(errno));
    if (file_info)
        hdfsFreeFileInfo(file_info, num);
    if (fs)
        if (hdfsDisconnect(fs))
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, 
                "get_and_head: hdfsDisconnect failed, %s", strerror(errno));
    return(ret);
}

/*
 * handle empty files or directories.
 */
static ngx_int_t
ngx_http_hdfs_handle_empty(ngx_http_request_t *r)
{
    ngx_buf_t   b;
    ngx_chain_t out;
    ngx_int_t   ret;

    r->headers_out.content_length_n = 0;

    if (!(b.pos = ngx_pcalloc(r->pool, 1))) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "handle_empty: "
                "ngx_pcalloc failed");
        return(NGX_HTTP_INTERNAL_SERVER_ERROR);
    }

    b.pos       = (u_char *)"";
    b.last      = b.pos + 1;
    b.memory    = 1;
    b.last_buf  = 1;
    out.buf     = &b;
    out.next    = NULL;

    if ((ret = ngx_http_output_filter(r, &out)) != NGX_OK) 
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "handle_empty: "
                "output_filter failed");
    return(ret);
}

/*
 * function for the directive 'hdfs'. parsing directive's args. 
 */
static char *
ngx_http_hdfs(ngx_conf_t *cf, ngx_command_t *cmd, void *void_conf)
{
    ngx_uint_t   i, l;
    ngx_str_t    *value;
    ngx_http_core_loc_conf_t *core_conf;
    ngx_http_hdfs_loc_conf_t *loc_conf;
 
    ngx_conf_log_error(NGX_LOG_DEBUG, cf, 0, "hdfs called");
    core_conf   = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    loc_conf    = (ngx_http_hdfs_loc_conf_t *)void_conf;
    core_conf->handler = ngx_http_hdfs_handler;

    value = cf->args->elts;
    for (i = 1; i < cf->args->nelts; i++) {
        if (!ngx_strncmp(value[i].data, NAMENODE, l = ngx_strlen(NAMENODE))) {
            loc_conf->namenode.data = &value[i].data[l];
            loc_conf->namenode.len  = ngx_strlen(&value[i].data[l]);
            continue;
        }
        if (!ngx_strncmp(value[i].data, USERNAME, l = ngx_strlen(USERNAME))) {
            loc_conf->username.data = &value[i].data[l];
            loc_conf->username.len  = ngx_strlen(&value[i].data[l]);
            continue;
        }
        if (!ngx_strncmp(value[i].data, PORT, l = ngx_strlen(PORT))) {
            loc_conf->port  = ngx_atoi(&value[i].data[l],
                    ngx_strlen(&value[i].data[l]));
            continue;
        }
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "hdfs: unknown option in "
                "configuration file, %s", value[i].data);
        return(NGX_CONF_ERROR);
    }
    if (!loc_conf->namenode.data) {
        loc_conf->namenode.data = (u_char *)strdup(NAMENODE_DEL);
        loc_conf->namenode.len  = ngx_strlen(NAMENODE_DEL);
    }
    ngx_conf_log_error(NGX_LOG_DEBUG, cf, 0, "after parsing directives in "
        "configuration file, namenode = %s, username = %s, port = %d",
        (loc_conf->namenode.data ? loc_conf->namenode.data : (u_char *)""),
        (loc_conf->username.data ? loc_conf->username.data : (u_char *)""),
        loc_conf->port);
    return(NGX_CONF_OK);
}

/*
 * get hdfs file path from uri.
 * e.g. uri = /hdfs/file.txt
 * return: /file.txt
 */
static u_char *
ngx_http_hdfs_get_path(u_char *uri)
{
    ngx_int_t   i, l;

    l = ngx_strlen(HDFS_PREFIX);
    for (i = l; uri[i] != ' '; i++)
        uri[i - l] = uri[i];
    uri[i - l] = 0;
    if (uri[0] == 0) { /* such as: /hdfs */
        uri[0] = '/';
        uri[1] = 0;
    }
    return(uri);
}

/*
 * readalbe premission check.
 */
static ngx_int_t
ngx_http_hdfs_rd_chk(ngx_http_request_t *r, hdfsFileInfo *info,
        ngx_str_t *username)
{
    short    perm = 0;

    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "rd_chk: "
            "info->mOwner = %s, username->data = %s", info->mOwner,
            username->data);
    if (!username->data || !username->len ||
            ngx_strcmp(info->mOwner, username->data))
        perm = info->mPermissions & 7;    /* others' permission */
    else 
        perm = (info->mPermissions >> 6) & 7;    /* user's permission */
    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0,
            "rd_chk: file permission = %d, perm=%d, %d", info->mPermissions,
            perm, info->mPermissions >> 6);
    return(perm & 4);
}

/*
 * vim: tw=80 ts=4 sts=4 sw=4 et
 */
