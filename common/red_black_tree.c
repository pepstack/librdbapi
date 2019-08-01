/******************************************************************************
* red_black_tree.c                                                            *
* Download From:                                                              *
*    http://www.cs.tau.ac.il/~efif/courses/Software1_Summer_03/code/rbtree/   *
* Last Edited by: cheungmine                                                  *
******************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "red_black_tree.h"

/*!
 * Operations on red_black_node_t struct
 */

/* Construct a red-black tree node */
red_black_node_t * rbnode_construct(void * object, red_black_color_enum color)
{
    red_black_node_t * node = (red_black_node_t *) malloc(sizeof(red_black_node_t));
    if (!node) {
        fprintf(stderr, "Not enough memory!\n");
        return 0;
    }
    node->object = object;
    node->color = color;
    node->parent = node->right = node->left = 0;
    return node;
}

/* Destructor of a red-black tree node */
void rbnode_destruct(red_black_node_t * node)
{
    if (!node) return;
    rbnode_destruct(node->right);
    rbnode_destruct(node->left);
    free(node);
}

/* Calculate the depth of the subtree spanned by a given node */
int rbnode_depth(red_black_node_t * node)
{
    /* Recursively calculate the depth of the left and right sub-trees */
    int  iRightDepth = (node->right) ? rbnode_depth(node->right) : 0;
    int  iLeftDepth = (node->left) ? rbnode_depth(node->left) : 0;

    /* Return the maximal child depth + 1 (the current node) */
    return ((iRightDepth > iLeftDepth) ? (iRightDepth + 1) : (iLeftDepth + 1));
}

/* Return the leftmost leaf in the tree */
red_black_node_t * rbnode_minimum(red_black_node_t * node)
{
    while (node->left)
        node = node->left;
    return node;
}

/* Return the rightmost leaf in the tree */
red_black_node_t * rbnode_maximum(red_black_node_t * node)
{
    while (node->right)
        node = node->right;
    return node;
}

/* Replace the object */
void rbnode_replace(red_black_node_t * node, void * object)
{
    /* Make sure the replacement does not violate the tree order */

    /* Replace the object at node */
    node->object = object;
}

/* Get the next node in the tree (according to the tree order) */
red_black_node_t * rbnode_successor(red_black_node_t * node)
{
    red_black_node_t * succ_node = 0;

    if (node) {
        if (node->right) {
            /* If there is a right child, the successor is the minimal object in
             * the sub-tree spanned by this child.
             */
            succ_node = node->right;
            while (succ_node->left)
                succ_node = succ_node->left;
        }
        else {
            /* Otherwise, go up the tree until reaching the parent from the left
             * direction.
             */
            red_black_node_t * prev_node = node;
            succ_node = node->parent;
            while (succ_node && prev_node == succ_node->right) {
                prev_node = succ_node;
                succ_node = succ_node->parent;
            }
        }
    }

    return (succ_node);
}

/* Get the previous node in the tree (according to the tree order) */
red_black_node_t * rbnode_predecessor(red_black_node_t * node)
{
    red_black_node_t * pred_node = 0;

    if (node) {
        if (node->left) {
            /* If there is a left child, the predecessor is the maximal object in
             * the sub-tree spanned by this child.
             */
            pred_node = node->left;
            while (pred_node->right)
                pred_node = pred_node->right;
        } else {
            /* Otherwise, go up the tree until reaching the parent from the right
             * direction.
             */
            red_black_node_t * prev_node = node;
            pred_node = node->parent;
            while (pred_node && prev_node == pred_node->left) {
                prev_node = pred_node;
                pred_node = pred_node->parent;
            }
        }
    }

    return (pred_node);
}

/* Return a pointer to a duplication of the given node */
red_black_node_t * rbnode_duplicate(red_black_node_t * node)
{
    /* Create a node of the same color, containing the same object */
    red_black_node_t * dup_node = rbnode_construct(node->object, node->color);
    if (!dup_node) return 0;

    /* Duplicate the children recursively */
    if (node->right) {
        dup_node->right = rbnode_duplicate (node->right);
        dup_node->right->parent = dup_node;
    } else {
        dup_node->right = 0;
    }

    if (node->left) {
        dup_node->left = rbnode_duplicate(node->left);
        dup_node->left->parent = dup_node;
    } else {
        dup_node->left = 0;
    }

    return dup_node;                      /* Return the duplicated node */
}

/* Traverse a red-black subtree */
void rbnode_traverse(red_black_node_t * node, fn_oper_func * opFunc, void* param)
{
    if (!node) return;
    rbnode_traverse(node->left, opFunc, param);
    opFunc(node->object, param);
    rbnode_traverse(node->right, opFunc, param);
}

/* Right-first traverse a red-black subtree */
void rbnode_traverse_right(red_black_node_t * node, fn_oper_func * opFunc, void* param)
{
    if (!node) return;
    rbnode_traverse_right(node->right, opFunc, param);
    opFunc(node->object, param);
    rbnode_traverse_right(node->left, opFunc, param);
}

/*
 * Operations on red_black_tree_t struct
 */

/* Intialize a tree */
void rbtree_init(red_black_tree_t * tree, fn_comp_func * keycmp)
{
    tree->keycmp = keycmp;
    tree->iSize = 0;
    tree->root = 0;
}

/* Construct a tree given a comparison function */
red_black_tree_t * rbtree_construct(fn_comp_func * keycmp)
{
    red_black_tree_t * tree = (red_black_tree_t *) malloc(sizeof(red_black_tree_t));
    if (!tree) {
        fprintf(stderr, "Not enough memory!\n");
        return 0;
    }
    rbtree_init(tree, keycmp);
    return tree;
}

/* Remove all objects from a black-red tree */
void rbtree_clean(red_black_tree_t * tree)
{
    if (tree->root)
        rbnode_destruct(tree->root);
    tree->root = 0;
    tree->iSize = 0;
}

/* Destruct a red-black tree */
void rbtree_destruct(red_black_tree_t * tree)
{
    rbtree_clean(tree);
    free(tree);
}

/* Returns the size of the tree */
int rbtree_size(red_black_tree_t * tree)
{
    return tree->iSize;
}

/* Returns the depth of the tree */
int rbtree_depth(red_black_tree_t * tree)
{
    if (!(tree->root))
        return 0;
    return rbnode_depth(tree->root);
}

/* Check whether the tree contains an object */
int rbtree_contains(red_black_tree_t * tree, void * object)
{
    return (rbtree_find(tree, object) != 0);
}

/* Insert an object to the tree */
red_black_node_t * rbtree_insert(red_black_tree_t * tree, void * object)
{
    red_black_node_t * cur_node;
    red_black_node_t * new_node = 0;

    if (!(tree->root)) {
        /* Assign a new root node. Notice that the root is always black */
        new_node = rbnode_construct(object, rbcBlack);
        if (!new_node) return 0;
        tree->root = new_node;
        tree->iSize = 1;
        return new_node;
    }

    /* Find a place for the new object, and insert it as a red leaf */
    cur_node = tree->root;

    while (cur_node) {
        /* Compare inserted object with the object stored in the current node */
        if (tree->keycmp(object, cur_node->object) > 0) {
            if (!(cur_node->left)) {
                /* Insert the new leaf as the left child of the current node */
                new_node = rbnode_construct(object, rbcRed);
                if (!new_node) return 0;

                cur_node->left = new_node;
                new_node->parent = cur_node;
                cur_node = 0;                /* terminate the while loop */
            } else {
                cur_node = cur_node->left;      /* Go to the left sub-tree */
            }
        } else {
            if (!(cur_node->right)) {
                /* Insert the new leaf as the right child of the current node */
                new_node = rbnode_construct(object, rbcRed);
                if (!new_node) return 0;
                cur_node->right = new_node;
                new_node->parent = cur_node;
                cur_node = 0;                /* terminate the while loop */
            } else {
                cur_node = cur_node->right;     /* Go to the right sub-tree */
            }
        }
    }

    /* Mark that a new node was added */
    tree->iSize++;

    /* Fix up the tree properties */
    rbtree_insert_fixup(tree, new_node);

    return new_node;
}


/* Insert an unique object to the tree */
red_black_node_t * rbtree_insert_unique(red_black_tree_t * tree, void * object, int *is_new_node)
{
    int cmp;
    red_black_node_t * cur_node;
    red_black_node_t * new_node = 0;

    *is_new_node = 1;

    if (!(tree->root)) {
        /* Assign a new root node. Notice that the root is always black */
        new_node = rbnode_construct(object, rbcBlack);
        if (!new_node) return 0;
        tree->root = new_node;
        tree->iSize = 1;
        return new_node;
    }

    /* Find a place for the new object, and insert it as a red leaf */
    cur_node = tree->root;

    while (cur_node) {
        cmp = tree->keycmp(object, cur_node->object);
        if (cmp == 0) {
            /* there already has an object with the same id as object to be inserted */
            *is_new_node = 0;
            return cur_node;
        }

        /* Compare inserted object with the object stored in the current node */
        if (cmp > 0) {
            if (!(cur_node->left)) {
                /* Insert the new leaf as the left child of the current node */
                new_node = rbnode_construct(object, rbcRed);
                if (!new_node)
                    return 0;
                cur_node->left = new_node;
                new_node->parent = cur_node;
                cur_node = 0;                /* terminate the while loop */
            } else {
                cur_node = cur_node->left;      /* Go to the left sub-tree */
            }
        } else {
            if (!(cur_node->right)) {
                /* Insert the new leaf as the right child of the current node */
                new_node = rbnode_construct(object, rbcRed);
                if (!new_node)
                    return 0;
                cur_node->right = new_node;
                new_node->parent = cur_node;
                cur_node = 0;                /* terminate the while loop */
            } else {
                cur_node = cur_node->right;     /* Go to the right sub-tree */
            }
        }
    }

    /* Mark that a new node was added */
    tree->iSize++;

    /* Fix up the tree properties */
    rbtree_insert_fixup(tree, new_node);

    return new_node;
}

/* Insert a new object to the tree as the a successor of a given node */
red_black_node_t * insert_successor_at(red_black_tree_t * tree,
                                     red_black_node_t * at_node, void * object)
{
    red_black_node_t * parent;
    red_black_node_t * new_node;

    if (!(tree->root)) {
        /* Assign a new root node. Notice that the root is always black */
        new_node = rbnode_construct(object, rbcBlack);
        if (!new_node) return 0;
        tree->root = new_node;
        tree->iSize = 1;
        return new_node;
    }

    /* Insert the new object as a red leaf, being the successor of node */
    new_node = rbnode_construct(object, rbcRed);
    if (!new_node) return 0;

    if (!at_node) {
        /* The new node should become the tree minimum: Place is as the left
         * child of the current minimal leaf.
         */
        parent = rbnode_minimum(tree->root);
        parent->left = new_node;
    } else {
        /* Make sure the insertion does not violate the tree order */

        /* In case given node has no right child, place the new node as its
         * right child. Otherwise, place it at the leftmost position at the
         * sub-tree rooted at its right side.
         */
        if (!at_node->right) {
            parent = at_node;
            parent->right = new_node;
        } else {
            parent = rbnode_minimum(at_node->right);
            parent->left = new_node;
        }
    }

    new_node->parent = parent;

    /* Mark that a new node was added */
    tree->iSize++;

    /* Fix up the tree properties */
    rbtree_insert_fixup(tree, new_node);

    return new_node;
}

/* Insert a new object to the tree as the a predecessor of a given node */
red_black_node_t * insert_predecessor_at(red_black_tree_t * tree,
                                       red_black_node_t * at_node, void * object)
{
    red_black_node_t * parent;
    red_black_node_t * new_node;

    if (!(tree->root)) {
        /* Assign a new root node. Notice that the root is always black */
        new_node = rbnode_construct(object, rbcBlack);
        if (!new_node) return 0;
        tree->root = new_node;
        tree->iSize = 1;
        return new_node;
    }

    /* Insert the new object as a red leaf, being the predecessor of at_node */
    new_node = rbnode_construct(object, rbcRed);
    if (!new_node) return 0;

    if (!at_node) {
        /* The new node should become the tree maximum: Place is as the right
         * child of the current maximal leaf.
         */
        parent = rbnode_maximum(tree->root);
        parent->right = new_node;
    } else {
        /* Make sure the insertion does not violate the tree order */

        /* In case given node has no left child, place the new node as its
         * left child. Otherwise, place it at the rightmost position at the
         * sub-tree rooted at its left side.
         */
        if (!(at_node->left)) {
            parent = at_node;
            parent->left = new_node;
        } else {
            parent = rbnode_maximum (at_node->left);
            parent->right = new_node;
        }
    }

    new_node->parent = parent;

    /* Mark that a new node was added */
    tree->iSize++;

    /* Fix up the tree properties */
    rbtree_insert_fixup(tree, new_node);

    return new_node;
}

/* Remove an object from the tree */
void rbtree_remove(red_black_tree_t * tree, void * object)
{
    red_black_node_t * node = rbtree_find(tree, object);    /* find the node */
    rbtree_remove_at(tree, node);                         /* remove the node */
}

/* Remove the object pointed by the given node. */
void rbtree_remove_at(red_black_tree_t * tree, red_black_node_t * node)
{
    red_black_node_t * child = 0;

    /* In case of deleting the single object stored in the tree, free the root,
     * thus emptying the tree.
     */
    if (tree->iSize == 1) {
        rbnode_destruct(tree->root);
        tree->root = 0;
        tree->iSize = 0;
        return;
    }

    /* Remove the given node from the tree */
    if (node->left && node->right) {
        /* If the node we want to remove has two children, find its successor,
         * which is the leftmost child in its right sub-tree and has at most
         * one child (it may have a right child).
         */
        red_black_node_t * succ_node = rbnode_minimum(node->right);

        /* Now physically swap node and its successor. Notice this may temporarily
         * violate the tree properties, but we are going to remove node anyway.
         * This way we have moved node to a position were it is more convinient
         * to delete it.
         */
        int immediate_succ = (node->right == succ_node);
        red_black_node_t * succ_parent = succ_node->parent;
        red_black_node_t * succ_left = succ_node->left;
        red_black_node_t * succ_right = succ_node->right;
        red_black_color_enum succ_color = succ_node->color;

        succ_node->parent = node->parent;
        succ_node->left = node->left;
        succ_node->right = immediate_succ ? node : node->right;
        succ_node->color = node->color;

        node->parent = immediate_succ ? succ_node : succ_parent;
        node->left = succ_left;
        node->right = succ_right;
        node->color = succ_color;

        if (!immediate_succ) {
            if (succ_node == node->parent->left)
                node->parent->left = node;
            else
                node->parent->right = node;
        }

        if (node->left)
            node->left->parent = node;
        if (node->right)
            node->right->parent = node;

        if (succ_node->parent) {
            if (node == succ_node->parent->left)
	            succ_node->parent->left = succ_node;
            else
	            succ_node->parent->right = succ_node;
        } else {
            tree->root = succ_node;
        }

        if (succ_node->left)
            succ_node->left->parent = succ_node;
        if (succ_node->right)
            succ_node->right->parent = succ_node;
    }

    /* At this stage, the node we are going to remove has at most one child */
    child = (node->left) ? node->left : node->right;

    /* Splice out the node to be removed, by linking its parent straight to the
     * removed node's single child.
     */
    if (child)
        child->parent = node->parent;

    if (!(node->parent)) {
        /* If we are deleting the root, make the child the new tree node */
        tree->root = child;
    } else {
        /* Link the removed node parent to its child */
        if (node == node->parent->left) {
            node->parent->left = child;
        } else {
            node->parent->right = child;
        }
    }

    /* Fix-up the red-black properties that may have been damaged: If we have
     * just removed a black node, the black-depth property is no longer valid.
     */
    if (node->color == rbcBlack && child)
        rbtree_remove_fixup(tree, child);

    /* Delete the un-necessary node (we 0 ify both its children because the
     * node's destructor is recursive).
     */
    node->left = 0;
    node->right = 0;
    free(node);

    /* Descrease the number of objects in the tree */
    tree->iSize--;
}

/* Get the tree minimum */
red_black_node_t * rbtree_minimum(red_black_tree_t * tree)
{
    if (!(tree->root))
        return 0;

    /* Return the leftmost leaf in the tree */
    return rbnode_minimum(tree->root);
}

/* Get the tree maximum */
red_black_node_t * rbtree_maximum(red_black_tree_t * tree)
{
    if (!(tree->root))
        return 0;

    /* Return the rightmost leaf in the tree */
    return rbnode_maximum(tree->root);
}

/* Return a pointer to the node containing the given object */
red_black_node_t * rbtree_find(red_black_tree_t * tree, void * object)
{
    red_black_node_t * cur_node = tree->root;
    int comp_result;

    while (cur_node) {
        /* In case of equality, we can return the current node. */
        if ((comp_result = tree->keycmp(object, cur_node->object)) == 0)
            return cur_node;
        /* Go down to the left or right child. */
        cur_node = (comp_result > 0) ? cur_node->left : cur_node->right;
    }

    /* If we reached here, the object is not found in the tree */
    return 0;
}

/* Left-rotate the sub-tree spanned by the given node:
 *
 *          |          RoateRight(y)            |
 *          y         -------------->           x
 *        /   \                               /   \       .
 *       x     T3       RoatateLeft(x)       T1    y      .
 *     /   \          <--------------            /   \    .
 *    T1    T2                                  T2    T3
 */
void rbtree_rotate_left(red_black_tree_t * tree, red_black_node_t * x_node)
{
    /* Get the right child of the node */
    red_black_node_t * y_node = x_node->right;

    /* Change its left subtree (T2) to x's right subtree */
    x_node->right = y_node->left;

    /* Link T2 to its new parent x */
    if (y_node->left != 0)
        y_node->left->parent = x_node;

    /* Assign x's parent to be y's parent */
    y_node->parent = x_node->parent;

    if (!(x_node->parent)) {
        /* Make y the new tree root */
        tree->root = y_node;
    } else  {
        /* Assign a pointer to y from x's parent */
        if (x_node == x_node->parent->left) {
            x_node->parent->left = y_node;
        }  else {
            x_node->parent->right = y_node;
        }
    }

    /* Assign x to be y's left child */
    y_node->left = x_node;
    x_node->parent = y_node;
}

/* Right-rotate the sub-tree spanned by the given node */
void rbtree_rotate_right(red_black_tree_t * tree, red_black_node_t * y_node)
{
    /* Get the left child of the node */
    red_black_node_t * x_node = y_node->left;

    /* Change its right subtree (T2) to y's left subtree */
    y_node->left = x_node->right;

    /* Link T2 to its new parent y */
    if (x_node->right != 0)
    x_node->right->parent = y_node;

    /* Assign y's parent to be x's parent */
    x_node->parent = y_node->parent;

    if (!(y_node->parent)) {
        /* Make x the new tree root */
        tree->root = x_node;
    } else  {
        /* Assign a pointer to x from y's parent */
        if (y_node == y_node->parent->left) {
            y_node->parent->left = x_node;
        } else {
            y_node->parent->right = x_node;
        }
    }

    /* Assign y to be x's right child */
    x_node->right = y_node;
    y_node->parent = x_node;
}

/* Fix-up the tree so it maintains the red-black properties after insertion */
void rbtree_insert_fixup(red_black_tree_t * tree, red_black_node_t * node)
{
    /* Fix the red-black propreties: we may have inserted a red leaf as the
     * child of a red parent - so we have to fix the coloring of the parent
     * recursively.
     */
    red_black_node_t * curr_node = node;
    red_black_node_t * grandparent;
    red_black_node_t *uncle;

    assert(node && node->color == rbcRed);

    while (curr_node != tree->root && curr_node->parent->color == rbcRed) {
        /* Get a pointer to the current node's grandparent (notice the root is
         * always black, so the red parent must have a parent).
         */
        grandparent = curr_node->parent->parent;

        if (curr_node->parent == grandparent->left) {
            /* If the red parent is a left child, the uncle is the right child of
             * the grandparent.
             */
            uncle = grandparent->right;

            if (uncle && uncle->color == rbcRed) {
                /* If both parent and uncle are red, color them black and color the
                 * grandparent red.
                 * In case of a 0 uncle, we treat it as a black node.
                 */
                curr_node->parent->color = rbcBlack;
                uncle->color = rbcBlack;
                grandparent->color = rbcRed;

                /* Move to the grandparent */
                curr_node = grandparent;
            } else {
                /* Make sure the current node is a right child. If not, left-rotate
                 * the parent's sub-tree so the parent becomes the right child of the
                 * current node (see _rotate_left).
                 */
                if (curr_node == curr_node->parent->right) {
                    curr_node = curr_node->parent;
                    rbtree_rotate_left(tree, curr_node);
                }

                /* Color the parent black and the grandparent red */
                curr_node->parent->color = rbcBlack;
                grandparent->color = rbcRed;

                /* Right-rotate the grandparent's sub-tree */
                rbtree_rotate_right(tree, grandparent);
            }
        } else {
            /* If the red parent is a right child, the uncle is the left child of
             * the grandparent.
             */
            uncle = grandparent->left;

            if (uncle && uncle->color == rbcRed) {
                /* If both parent and uncle are red, color them black and color the
                 * grandparent red.
                 * In case of a 0 uncle, we treat it as a black node.
                 */
                curr_node->parent->color = rbcBlack;
                uncle->color = rbcBlack;
                grandparent->color = rbcRed;

                /* Move to the grandparent */
                curr_node = grandparent;
            } else {
                /* Make sure the current node is a left child. If not, right-rotate
                 * the parent's sub-tree so the parent becomes the left child of the
                 * current node.
                 */
                if (curr_node == curr_node->parent->left) {
                    curr_node = curr_node->parent;
                    rbtree_rotate_right(tree, curr_node);
                }

                /* Color the parent black and the grandparent red */
                curr_node->parent->color = rbcBlack;
                grandparent->color = rbcRed;

                /* Left-rotate the grandparent's sub-tree */
                rbtree_rotate_left(tree, grandparent);
            }
        }
    }

    /* Make sure that the root is black */
    tree->root->color = rbcBlack;
}

void rbtree_remove_fixup(red_black_tree_t * tree, red_black_node_t * node)
{
    red_black_node_t * curr_node = node;
    red_black_node_t * sibling;

    while (curr_node != tree->root && curr_node->color == rbcBlack) {
        /* Get a pointer to the current node's sibling (notice that the node's
         * parent must exist, since the node is not the root).
         */
        if (curr_node == curr_node->parent->left) {
            /* If the current node is a left child, its sibling is the right
             * child of the parent.
             */
            sibling = curr_node->parent->right;

            /* Check the sibling's color. Notice that 0 nodes are treated
             * as if they are colored black.
             */
            if (sibling && sibling->color == rbcRed) {
                /* In case the sibling is red, color it black and rotate.
                 * Then color the parent red (and the grandparent is now black).
                 */
                sibling->color = rbcBlack;
                curr_node->parent->color = rbcRed;
                rbtree_rotate_left(tree, curr_node->parent);
                sibling = curr_node->parent->right;
            }

            if (sibling &&
                (!(sibling->left) || sibling->left->color == rbcBlack) &&
                (!(sibling->right) || sibling->right->color == rbcBlack))
            {
                /* If the sibling has two black children, color it red */
                sibling->color = rbcRed;
                if (curr_node->parent->color == rbcRed) {
                    /* If the parent is red, we can safely color it black and terminate
                     * the fix-up process.
                     */
                    curr_node->parent->color = rbcBlack;
                    curr_node = tree->root;      /* In order to stop the while loop */
                } else {
                    /* The black depth of the entire sub-tree rooted at the parent is
                     * now too small - fix it up recursively.
                     */
                    curr_node = curr_node->parent;
                }
            } else {
                if (!sibling) {
                    /* Take special care of the case of a 0 sibling */
                    if (curr_node->parent->color == rbcRed) {
                        curr_node->parent->color = rbcBlack;
                        curr_node = tree->root;    /* In order to stop the while loop */
                    } else {
                        curr_node = curr_node->parent;
                    }
                } else {
                    /* In this case, at least one of the sibling's children is red.
                     * It is therfore obvious that the sibling itself is black.
                     */
                    if (sibling->right && sibling->right->color == rbcRed) {
                        /* If the right child of the sibling is red, color it black and
                         * rotate around the current parent.
                         */
                        sibling->right->color = rbcBlack;
                        rbtree_rotate_left(tree, curr_node->parent);
                    } else {
                        /* If the left child of the sibling is red, rotate around the
                         * sibling, then rotate around the new sibling of our current
                         * node.
                         */
                        rbtree_rotate_right(tree, sibling);
                        sibling = curr_node->parent->right;
                        rbtree_rotate_left(tree, sibling);
                    }

                    /* It is now safe to color the parent black and to terminate the
                     * fix-up process.
                     */
                    if (curr_node->parent->parent)
                        curr_node->parent->parent->color = curr_node->parent->color;
                    curr_node->parent->color = rbcBlack;
                    curr_node = tree->root;      /* In order to stop the while loop */
                }
            }
        } else {
            /* If the current node is a right child, its sibling is the left
             * child of the parent.
             */
            sibling = curr_node->parent->left;

            /* Check the sibling's color. Notice that 0 nodes are treated
             * as if they are colored black.
             */
            if (sibling && sibling->color == rbcRed) {
                /* In case the sibling is red, color it black and rotate.
                 * Then color the parent red (and the grandparent is now black).
                 */
                sibling->color = rbcBlack;
                curr_node->parent->color = rbcRed;
                rbtree_rotate_right(tree, curr_node->parent);

                sibling = curr_node->parent->left;
            }

            if (sibling &&
                (!(sibling->left) || sibling->left->color == rbcBlack) &&
                (!(sibling->right) || sibling->right->color == rbcBlack))
            {
                /* If the sibling has two black children, color it red */
                sibling->color = rbcRed;
                if (curr_node->parent->color == rbcRed) {
                    /* If the parent is red, we can safely color it black and terminate
                     * the fix-up process.
                     */
                    curr_node->parent->color = rbcBlack;
                    curr_node = tree->root;      /* In order to stop the while loop */
                } else {
                    /* The black depth of the entire sub-tree rooted at the parent is
                     * now too small - fix it up recursively.
                     */
                    curr_node = curr_node->parent;
                }
            } else {
                if (!sibling) {
                    /* Take special care of the case of a 0 sibling */
                    if (curr_node->parent->color == rbcRed) {
                        curr_node->parent->color = rbcBlack;
                        curr_node = tree->root;    /* In order to stop the while loop */
                    } else {
                        curr_node = curr_node->parent;
                    }
                } else {
                    /* In this case, at least one of the sibling's children is red.
                     * It is therfore obvious that the sibling itself is black.
                     */
                    if (sibling->left && sibling->left->color == rbcRed) {
                        /* If the left child of the sibling is red, color it black and
                         * rotate around the current parent
                         */
                        sibling->left->color = rbcBlack;
                        rbtree_rotate_right(tree, curr_node->parent);
                    } else {
                        /* If the right child of the sibling is red, rotate around the
                         * sibling, then rotate around the new sibling of our current
                         * node
                         */
                        rbtree_rotate_left(tree, sibling);
                        sibling = curr_node->parent->left;
                        rbtree_rotate_right(tree, sibling);
                    }

                    /* It is now safe to color the parent black and to terminate the
                     * fix-up process.
                     */
                    if (curr_node->parent->parent)
                        curr_node->parent->parent->color = curr_node->parent->color;
                    curr_node->parent->color = rbcBlack;
                    curr_node = tree->root;       /* In order to stop the while loop */
                }
            }
        }
    }

    /* The root can always be colored black */
    curr_node->color = rbcBlack;
}

/* Traverse a red-black tree */
void rbtree_traverse(red_black_tree_t * tree, fn_oper_func * op, void *param)
{
    rbnode_traverse(tree->root, op, param);
}

/* Right-first traverse a red-black tree */
void rbtree_traverse_right(red_black_tree_t * tree, fn_oper_func * op, void *param)
{
    rbnode_traverse_right(tree->root, op, param);
}