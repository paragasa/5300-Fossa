#include "btree.h"

BTreeIndex::BTreeIndex(DbRelation &relation, Identifier name, ColumnNames key_columns, bool unique)
	: DbIndex(relation, name, key_columns, unique),
	  closed(true),
	  stat(nullptr),
	  root(nullptr),
	  file(relation.get_table_name() + "-" + name),
	  key_profile()
{
	if (!unique)
		throw DbRelationError("BTree index must have unique key");
	// FIXME - what else?!
	this->build_key_profile();
}

BTreeIndex::~BTreeIndex()
{
	// FIXME - free up stuff
	this->close();
}

// Create the index.
void BTreeIndex::create()
{
	// FIXME
	this->file.create();

	this->stat = new BTreeStat(this->file, STAT, STAT + 1, this->key_profile);
	this->root = new BTreeLeaf(this->file, STAT + 1, this->key_profile, true);
	this->closed = false;

	Handles *handles = this->relation.select();
	for (auto const &handle : *handles)
	{
		this->insert(handle);
	}

	delete handles;
}

// Drop the index.
void BTreeIndex::drop()
{
	// FIXME
	this->close();
	this->file.drop();
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open()
{
	if (this->closed)
	{
		this->file.open();

		this->stat = new BTreeStat(this->file, STAT, this->key_profile);

		if (this->stat->get_height() == 1)
		{
			this->root = new BTreeLeaf(this->file, stat->get_root_id(), this->key_profile, false);
		}
		else
		{
			this->root = new BTreeInterior(this->file, stat->get_root_id(), this->key_profile, false);
		}

		this->closed = false;
	}
}

// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeIndex::close()
{
	if (!this->closed)
	{
		this->file.close();
		this->closed = true;
	}

	if (this->stat != NULL)
	{
		delete this->stat;
		this->stat = NULL;
	}

	if (this->root != NULL)
	{
		delete this->root;
		this->root = NULL;
	}
}

// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles *BTreeIndex::lookup(ValueDict *key_dict) const
{
	// FIXME
	KeyValue *key_value = this->tkey(key_dict);

	try
	{
		Handles *handles = this->_lookup(this->root, this->stat->get_height(), key_value);
		delete key_value;
		return handles;
	}
	catch (DbRelationError &exception)
	{
		delete key_value;
		throw;
	}
}

Handles *BTreeIndex::range(ValueDict *min_key, ValueDict *max_key) const
{
	throw DbRelationError("Don't know how to do a range query on Btree index yet");
	// FIXME
}

// Insert a row with the given handle. Row must exist in relation already.
void BTreeIndex::insert(Handle handle)
{
	// FIXME
	ValueDict *value_dict = this->relation.project(handle, &this->key_columns);
	KeyValue *key_value = this->tkey(value_dict);
	delete value_dict;

	Insertion insertion = this->_insert(this->root, this->stat->get_height(), key_value, handle);
	delete key_value;

	if (!BTreeNode::insertion_is_none(insertion))
	{
		BTreeInterior *btree_interior = new BTreeInterior(this->file, 0, this->key_profile, true);
		btree_interior->set_first(this->root->get_id());
		btree_interior->insert(&insertion.second, insertion.first);
		delete this->root;
		this->root = btree_interior;
		this->stat->set_root_id(this->root->get_id());
		this->stat->set_height(this->stat->get_height() + 1);
		this->stat->save();
	}
}

void BTreeIndex::del(Handle handle)
{
	throw DbRelationError("Don't know how to delete from a BTree index yet");
	// FIXME
}

KeyValue *BTreeIndex::tkey(const ValueDict *key) const
{
	KeyValue *key_value = new KeyValue();

	for (uint i = 0; i < this->key_columns.size(); i++)
	{
		Identifier identifier = key_columns[i];

		if (key->find(identifier) == key->end())
		{
			delete key_value;
			throw DbRelationError("Cannot find one of the key columns: " + identifier);
		}

		if (key->at(identifier).data_type != this->key_profile[i])
		{
			delete key_value;
			throw DbRelationError("The value type of " + identifier + " does not match");
		}

		key_value->push_back(key->at(identifier));
	}

	return key_value;
}

void BTreeIndex::build_key_profile()
{
	ColumnAttributes *column_attributes = relation.get_column_attributes(key_columns);

	for (auto &column_attribute : *column_attributes)
	{
		key_profile.push_back(column_attribute.get_data_type());
	}

	delete column_attributes;
}

Handles *BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue *key) const
{
	if (height == 1)
	{
		BTreeLeaf *btree_leaf = static_cast<BTreeLeaf *>(node);
		Handle handle = btree_leaf->find_eq(key);

		return new Handles(1, handle);
	}
	else
	{
		BTreeInterior *btree_interior = static_cast<BTreeInterior *>(node);
		BTreeNode *next = NULL;

		try
		{
			next = btree_interior->find(key, height);
			Handles *handles = this->_lookup(next, height - 1, key);
			delete next;
			return handles;
		}
		catch (DbRelationError &exception)
		{
			if (next != NULL)
			{
				delete next;
			}

			throw;
		}
	}
}

Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue *key, Handle handle)
{
	if (height == 1)
	{
		BTreeLeaf *btree_leaf = static_cast<BTreeLeaf *>(node);
		return btree_leaf->insert(key, handle);
	}
	else
	{
		BTreeInterior *btree_interior = static_cast<BTreeInterior *>(node);
		BTreeNode *next = btree_interior->find(key, height);
		Insertion insertion = _insert(next, height - 1, key, handle);
		delete next;

		if (!BTreeNode::insertion_is_none(insertion))
		{
			return btree_interior->insert(&insertion.second, insertion.first);
		}

		return insertion;
	}
}
