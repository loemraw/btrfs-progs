#include "check/mode-common.h"
#include "check/repair.h"
#include "common/clear-cache.h"
#include "kerncompat.h"
#include "kernel-lib/rbtree.h"
#include "kernel-shared/accessors.h"
#include "kernel-shared/ctree.h"
#include "kernel-shared/disk-io.h"
#include "kernel-shared/file-item.h"
#include "kernel-shared/transaction.h"
#include "kernel-shared/uapi/btrfs_tree.h"
#include "kernel-shared/volumes.h"
#include <stdio.h>
#include <stdlib.h>

#define PROCESS_FREE_SPACE_TREE_ELEMENT_FLAG (1ULL << 0)
#define PROCESS_EXTENT_TREE_ELEMENT_FLAG (1ULL << 1)

static struct btrfs_root *get_free_space_root(struct btrfs_root *root) {
  struct rb_node *n;
  struct btrfs_key key = {
      .objectid = BTRFS_FREE_SPACE_TREE_OBJECTID,
      .type = BTRFS_ROOT_ITEM_KEY,
      .offset = 0,
  };
  int ret = 0;
  struct btrfs_root *free_space_root = NULL;

  free_space_root = btrfs_global_root(root->fs_info, &key);
  while (1) {
    if (ret)
      break;
    n = rb_next(&root->rb_node);
    if (!n)
      break;
    free_space_root = rb_entry(n, struct btrfs_root, rb_node);
    if (root->root_key.objectid != BTRFS_FREE_SPACE_TREE_OBJECTID)
      break;
    printf("WARNING multiple free_space_roots");
  }
  return free_space_root;
}

static struct btrfs_root *get_extent_root(struct btrfs_root *root) {
  struct rb_node *n;
  struct btrfs_key key = {
      .objectid = BTRFS_EXTENT_TREE_OBJECTID,
      .type = BTRFS_ROOT_ITEM_KEY,
      .offset = 0,
  };
  int ret = 0;
  struct btrfs_root *extent_root = NULL;

  extent_root = btrfs_global_root(root->fs_info, &key);
  while (1) {
    if (ret)
      break;
    n = rb_next(&root->rb_node);
    if (!n)
      break;
    extent_root = rb_entry(n, struct btrfs_root, rb_node);
    if (root->root_key.objectid != BTRFS_FREE_SPACE_TREE_OBJECTID)
      break;
    printf("WARNING multiple free_space_roots");
  }
  return extent_root;
}

static int next_element(struct btrfs_root *root, struct btrfs_path *path,
                        struct btrfs_key *key, struct extent_buffer **leaf,
                        int *slot) {
  int ret;
  *leaf = path->nodes[0];
  *slot = path->slots[0];

  if (*slot >= btrfs_header_nritems(*leaf)) {
    ret = btrfs_next_leaf(root, path);
    if (ret) {
      return 1; // done walking
    }
    *leaf = path->nodes[0];
    *slot = path->slots[0];
  }

  btrfs_item_key_to_cpu(*leaf, key, *slot);

  ++path->slots[0];

  return 0;
}

static int process_free_space_info(struct btrfs_key *key, u64 *start,
                                   u64 *end) {
  if (key->type != BTRFS_FREE_SPACE_INFO_KEY)
    return 0;

  *start = key->objectid;
  *end = key->objectid + key->offset;

  printf("processed FREE_SPACE_INFO %lld-%lld\n", *start, *end);

  return 1;
}

static int process_free_space_extent(struct btrfs_key *key, u64 *start,
                                     u64 *end) {
  if (key->type == BTRFS_FREE_SPACE_EXTENT_KEY) {
    *start = key->objectid;
    *end = key->objectid + key->offset;

    printf("processed FREE_SPACE_EXTENT %lld-%lld\n", *start, *end);
  } else if (key->type == BTRFS_FREE_SPACE_BITMAP_KEY) {
    printf("TODO: process BTRFS_FREE_SPACE_BITMAP\n");
  }

  return 1;
}

static int process_extent(struct btrfs_key *key, u64 *start, u64 *end,
                          u32 metadata_size) {
  if (key->type == BTRFS_EXTENT_ITEM_KEY) {
    *start = key->objectid;
    *end = key->objectid + key->offset;

    printf("processed EXTENT %lld-%lld\n", *start, *end);
    return 1;
  } else if (key->type == BTRFS_METADATA_ITEM_KEY) {
    *start = key->objectid;
    *end = key->objectid + metadata_size;

    printf("processed METADATA EXTENT %lld-%lld\n", *start, *end);
    return 1;
  }

  return 0;
}

static int walk_trees(struct btrfs_root *free_space_root,
                      struct btrfs_root *extent_root) {
  struct btrfs_key key = {0};
  struct btrfs_path fs_path = {0};
  struct btrfs_path e_path = {0};
  struct extent_buffer *leaf = NULL;
  int slot;
  int ret;

  int metadata_size = extent_root->fs_info->nodesize;

  int fs_walk_done = 0;
  int e_walk_done = 0;

  u64 process =
      PROCESS_EXTENT_TREE_ELEMENT_FLAG | PROCESS_FREE_SPACE_TREE_ELEMENT_FLAG;

  u64 bg_start = -1;
  u64 bg_end = -1;
  u64 end = -1;
  u64 fs_start = -1;
  u64 fs_end = -1;
  u64 e_start = -1;
  u64 e_end = -1;

  int filled_bg = -1;

  ret = btrfs_search_slot(NULL, free_space_root, &key, &fs_path, 0, 0);
  if (ret < 0)
    goto error;

  ret = btrfs_search_slot(NULL, extent_root, &key, &e_path, 0, 0);
  if (ret < 0)
    goto error;

  while (!fs_walk_done || !e_walk_done) {
    // step 1 find FS_INFO
    while (!fs_walk_done &&
           !(fs_walk_done =
                 next_element(free_space_root, &fs_path, &key, &leaf, &slot))) {
      if (process_free_space_info(&key, &bg_start, &bg_end)) {
        filled_bg = 0;
        break;
      }
      printf("ERROR not found FREE_SPACE_INFO\n");
      ret = -1;
      goto error;
    }

    // step 2 find FS_EXTENT and EXTENT
    if (process & PROCESS_FREE_SPACE_TREE_ELEMENT_FLAG) {
      while (!fs_walk_done &&
             !(fs_walk_done = next_element(free_space_root, &fs_path, &key,
                                           &leaf, &slot))) {
        if (process_free_space_extent(&key, &fs_start, &fs_end))
          break;
      }
    }

    if (process & PROCESS_EXTENT_TREE_ELEMENT_FLAG) {
      while (!e_walk_done && !(e_walk_done = next_element(
                                   extent_root, &e_path, &key, &leaf, &slot))) {
        if (process_extent(&key, &e_start, &e_end, metadata_size))
          break;
      }
    }

    if (fs_walk_done) {
      if (!e_walk_done) {
        printf("ERROR extent(s) remaining after final block group\n");
        ret = -1;
        goto error;
      }
      printf("finished walking\n");
      break;
    }

    // step 3 determine starting fs or extent.
    if (fs_start < bg_start) {
      printf("ERROR free space %lld-%lld starts before block group %lld-%lld\n",
             fs_start, fs_end, bg_start, bg_end);
      ret = -1;
      goto error;
    }

    if (e_start < bg_start) {
      printf("ERROR extent %lld-%lld starts before block group %lld-%lld\n",
             e_start, e_end, bg_start, bg_end);
      ret = -1;
      goto error;
    }

    if (fs_start == bg_start) {
      printf("start of bg %lld-%lld is free_space_extent %lld-%lld\n", bg_start,
             bg_end, fs_start, fs_end);
      end = fs_end;
      process = PROCESS_FREE_SPACE_TREE_ELEMENT_FLAG;
    } else if (e_start == bg_start) {
      printf("start of bg %lld-%lld is extent %lld-%lld\n", bg_start, bg_end,
             e_start, e_end);
      end = e_end;
      process = PROCESS_EXTENT_TREE_ELEMENT_FLAG;
    } else {
      printf("ERROR gap at the start of block group\n");
      ret = -1;
      goto error;
    }

    while (1) {
      if (end == bg_end) {
        printf("ended bg %lld-%lld\n", bg_start, bg_end);
        filled_bg = 1;
        break;
      }

      if (end > bg_end) {
        printf(
            "ERROR extent range ends %lld outside of block group %lld-%lld\n",
            end, bg_start, bg_end);
        ret = -1;
        goto error;
      }

      if (process & PROCESS_FREE_SPACE_TREE_ELEMENT_FLAG) {
        while (!fs_walk_done &&
               !(fs_walk_done = next_element(free_space_root, &fs_path, &key,
                                             &leaf, &slot))) {
          if (process_free_space_extent(&key, &fs_start, &fs_end))
            break;
        }
      } else if (process & PROCESS_EXTENT_TREE_ELEMENT_FLAG) {
        while (!e_walk_done &&
               !(e_walk_done =
                     next_element(extent_root, &e_path, &key, &leaf, &slot))) {
          if (process_extent(&key, &e_start, &e_end, metadata_size))
            break;
        }
      }

      if (fs_start == end) {
        printf("next is free_space %lld-%lld\n", fs_start, fs_end);
        end = fs_end;
        process = PROCESS_FREE_SPACE_TREE_ELEMENT_FLAG;
      } else if (e_start == end) {
        printf("next is extent %lld-%lld\n", e_start, e_end);
        end = e_end;
        process = PROCESS_EXTENT_TREE_ELEMENT_FLAG;
      } else {
        printf("ERROR gap in block group %lld-%lld, expected something "
               "starting at "
               "%lld\n",
               bg_start, bg_end, end);
        ret = -1;
        goto error;
      }
    }
  }

  if (!filled_bg) {
    printf("ERROR did not fill bg %lld-%lld\n", bg_start, bg_end);
    ret = -1;
    goto error;
  }

  ret = 0;
error:
  btrfs_release_path(&fs_path);
  btrfs_release_path(&e_path);
  return ret;
}

static int check_block_group_overlap(struct btrfs_root *root) {
  struct btrfs_root *free_space_root;
  struct btrfs_root *extent_root;

  free_space_root = get_free_space_root(root);
  if (IS_ERR_OR_NULL(free_space_root))
    return -1;

  extent_root = get_extent_root(root);
  if (IS_ERR_OR_NULL(extent_root))
    return -1;

  walk_trees(free_space_root, extent_root);

  return 0;
}
