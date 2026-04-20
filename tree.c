// tree.c — Tree object serialization and construction
//
// PROVIDED functions: get_file_mode, tree_parse, tree_serialize
// TODO functions:     tree_from_index
//
// Binary tree format (per entry, concatenated with no separators):
//   "<mode-as-ascii-octal> <name>\0<32-byte-binary-hash>"

#include "tree.h"
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

// Forward declaration
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;
    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1;

        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);

        ptr = space + 1;

        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1;

        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0';

        ptr = null_byte + 1;

        if (ptr + HASH_SIZE > end) return -1;
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    size_t max_size = tree->count * 296;
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];
        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += written + 1;
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── IMPLEMENTED ─────────────────────────────────────────────────────────────

// Recursive helper: builds one level of the tree from a slice of index entries.
//
// entries  - array of index entries
// count    - number of entries in this slice
// prefix   - the directory prefix this level is responsible for (e.g., "src/")
//            empty string ("") for the root level
// id_out   - receives the ObjectID of the written tree object
//
// All paths in `entries` at this call must already have `prefix` stripped off.
static int write_tree_level(IndexEntry *entries, int count,
                            const char *prefix, ObjectID *id_out) {
    Tree tree;
    tree.count = 0;

    int i = 0;
    while (i < count) {
        const char *path = entries[i].path;

        // Strip the current prefix from the path
        size_t prefix_len = strlen(prefix);
        const char *rel = path + prefix_len;

        // Does this entry live in a subdirectory at this level?
        const char *slash = strchr(rel, '/');

        if (slash == NULL) {
            // Plain file entry at this level
            TreeEntry *te = &tree.entries[tree.count++];
            te->mode = entries[i].mode;
            te->hash = entries[i].hash;
            strncpy(te->name, rel, sizeof(te->name) - 1);
            te->name[sizeof(te->name) - 1] = '\0';
            i++;
        } else {
            // Directory entry: collect all entries that share this subdirectory name
            size_t dir_name_len = (size_t)(slash - rel);
            char dir_name[256];
            strncpy(dir_name, rel, dir_name_len);
            dir_name[dir_name_len] = '\0';

            // Build the sub-prefix: prefix + dir_name + "/"
            char sub_prefix[512];
            snprintf(sub_prefix, sizeof(sub_prefix), "%s%s/", prefix, dir_name);
            size_t sub_prefix_len = strlen(sub_prefix);

            // Count how many entries belong to this subdirectory
            int j = i;
            while (j < count && strncmp(entries[j].path, sub_prefix, sub_prefix_len) == 0) {
                j++;
            }

            // Recurse to build the subtree for this directory
            ObjectID sub_id;
            if (write_tree_level(entries + i, j - i, sub_prefix, &sub_id) != 0)
                return -1;

            // Add a directory entry to the current tree
            TreeEntry *te = &tree.entries[tree.count++];
            te->mode = MODE_DIR;
            te->hash = sub_id;
            strncpy(te->name, dir_name, sizeof(te->name) - 1);
            te->name[sizeof(te->name) - 1] = '\0';

            i = j; // Skip past all entries we just consumed
        }
    }

    // Serialize and write this tree level to the object store
    void *data;
    size_t len;
    if (tree_serialize(&tree, &data, &len) != 0) return -1;

    int rc = object_write(OBJ_TREE, data, len, id_out);
    free(data);
    return rc;
}

// Comparison function for sorting index entries by path (for tree building)
static int compare_index_by_path(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

// Build a tree hierarchy from the current index and write all tree
// objects to the object store.
//
// Returns 0 on success, -1 on error.
int tree_from_index(ObjectID *id_out) {
    // Read .pes/index directly so tree.o has no dependency on index.o
    // (test_tree doesn't link index.o per the provided Makefile)
    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) return -1;

    IndexEntry entries[MAX_INDEX_ENTRIES];
    int count = 0;

    while (count < MAX_INDEX_ENTRIES) {
        IndexEntry *e = &entries[count];
        char hex[HASH_HEX_SIZE + 1];
        unsigned long long mtime;
        unsigned int size;
        int n = fscanf(f, "%o %64s %llu %u %511s\n",
                       &e->mode, hex, &mtime, &size, e->path);
        if (n != 5) break;
        if (hex_to_hash(hex, &e->hash) != 0) break;
        e->mtime_sec = (uint64_t)mtime;
        e->size      = (uint32_t)size;
        count++;
    }
    fclose(f);

    return write_tree_level(entries, count, "", id_out);
}

