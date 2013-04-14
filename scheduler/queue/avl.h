// -*- c++ -*-
#ifndef SWAN_QUEUE_AVL_H
#define SWAN_QUEUE_AVL_H

#include <algorithm>

template<typename NodeType, typename KeyType>
class avl_tree;

enum avl_dir_t { dir_left = 0, dir_right = 1 };

inline avl_dir_t opposite( avl_dir_t dir ) {
    return avl_dir_t( 1 - dir );
}

enum avl_cmp_t { cmp_lt=-1, cmp_eq=0, cmp_gt=1 };

template<typename NodeType, typename KeyType>
struct avl_traits {
    static NodeType * & left( NodeType * n );
    static NodeType * & right( NodeType * n );
    static NodeType * & child( NodeType * n, avl_dir_t dir );
    static avl_cmp_t compare( NodeType * l, NodeType * r );
    static avl_cmp_t compare( NodeType * l, KeyType & k );
    static short & balance( NodeType * n );
};

template<typename NodeType, typename KeyType>
class avl_tree {
    typedef NodeType node_t;
    typedef KeyType  key_t;
    typedef avl_traits<node_t, key_t> traits;
    typedef avl_dir_t dir_t;
    typedef avl_cmp_t cmp_t;

    node_t * root;

public:
    avl_tree() : root( 0 ) { }

    void insert( node_t * n ) {
	short change = 0;
	traits::left( n ) = 0;
	traits::right( n ) = 0;
	traits::balance( n ) = 0;
	insert_helper( root, n, change );
    }

    void remove( node_t * n ) {
	short change = 0;
	remove_helper( root, n, change );
    }

    node_t * find( key_t & k ) const {
	return find_helper( root, k );
    }

private:
    static node_t * find_helper( node_t * root, key_t & k ) {
	if( !root )
	    return 0;
	cmp_t cmp = traits::compare( root, k );
	if( cmp == cmp_eq )
	    return root;
	return find_helper( traits::child( root, cmp == cmp_lt
					   ? dir_left : dir_right ), k );
    }

    static bool insert_helper( node_t * & root, node_t * n, short & change ) {
	if( !root ) {
	    root = n;
	    change = 1;
	    return false;
	}

	int increase = 0;

	cmp_t result = traits::compare( root, n );

	if( result == cmp_eq )
	    return true;

	bool found;
	if( result == cmp_lt )
	    found = insert_helper( traits::left( root ), n, change );
	else
	    found = insert_helper( traits::right( root ), n, change );
	// Key already present
	if( found )
	    return found;

	increase = result * change;
	traits::balance( root ) += increase;

	change = ( increase && traits::balance( root ) != 0 )
	    ? (  1 - rebalance( root ) ) : 0;

	return false;
    }

    static int rebalance( node_t * & root ) {
	if( traits::balance( root ) < -1 ) {
	    // Right rotation
	    if( traits::balance( traits::left( root ) ) == 1 ) {
		// RL rotation
		return rotate_twice( root, dir_right );
	    } else {
		// RR rotation
		return rotate_once( root, dir_right );
	    }
	} else if( traits::balance( root ) > 1 ) {
	    // Left rotation
	    if( traits::balance( traits::right( root ) ) == -1 ) {
		// LR rotation
		return rotate_twice( root, dir_left );
	    } else {
		// LL rotation
		return rotate_once( root, dir_left );
	    }
	}

	return 0;
    }

    static short rotate_once( node_t * & root, dir_t dir ) {
	dir_t other = opposite( dir );
	node_t * old_root = root;

	short change = traits::balance( traits::child( root, other ) ) != 0;

	root = traits::child( root, other );

	traits::child( old_root, other ) = traits::child( root, dir );
	traits::child( root, dir ) = old_root;

	traits::balance( root ) += (dir == dir_left ? -1 : 1);
	traits::balance( old_root ) = -traits::balance(root);

	return change;
    }

    static short rotate_twice( node_t * & root, dir_t dir ) {
	dir_t other = opposite( dir );
	node_t * old_root = root;
	node_t * old_other = traits::child( root, other );

	root = traits::child( traits::child( old_root, other ), dir );
	traits::child( root, dir ) = old_root;

	traits::child( old_other, dir ) = traits::child( root, other );
	traits::child( root, other ) = old_other;

	traits::balance( traits::left( root ) )
	    = -std::max( traits::balance( root ), short(0) );
	traits::balance( traits::right( root ) )
	    = -std::min( traits::balance( root ), short(0) );
	traits::balance( root ) = 0;

	return 1;
    }

    static bool remove_helper( node_t * & root, node_t * n, short & change ) {
	if( !root ) {
	    change = 0;
	    return false;
	}

	int decrease = 0;
	bool found = false;

	cmp_t result = traits::compare( root, n );
	dir_t dir = result == cmp_lt ? dir_left : dir_right;

	if( result != cmp_eq ) {
	    found = remove_helper( traits::child( root, dir ), n, change );
	    if( !found )
		return found;
	    decrease = result * change;
	} else {
	    found = true;

	    if( !traits::left( root ) && !traits::right( root ) ) {
		root = 0;
		change = 1;
		return found;
	    } else if( !traits::left( root ) ) {
		node_t * to_delete = root;
		root = traits::right( root );
		traits::left( to_delete ) = 0;
		traits::right( to_delete ) = 0;
		return found;
	    } else if( !traits::right( root ) ) {
		node_t * to_delete = root;
		root = traits::left( root );
		traits::left( to_delete ) = 0;
		traits::right( to_delete ) = 0;
		return found;
	    } else {
		node_t * tmp = traits::right( root );
		node_t * parent = 0;

		while( traits::left( tmp ) ) {
		    parent = tmp;
		    tmp = traits::left( tmp );
		}

		if( parent )
		    remove_helper( traits::left( parent ),
				   traits::left( parent ), change );
		else
		    remove_helper( traits::right( root ),
				   traits::right( root ), change );

		traits::left( tmp ) = traits::left( n );
		traits::right( tmp ) = traits::right( n );
	    }
	}

	if( decrease ) {
	    traits::balance( root ) -= decrease;
	    if( traits::balance( root ) )
		change = rebalance( root );
	    else
		change = 1;
	} else {
	    change = 0;
	}

	return found;
    }
};

#endif // SWAN_QUEUE_AVL_H
