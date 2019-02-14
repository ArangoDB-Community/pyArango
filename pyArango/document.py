import json, types
from .theExceptions import (CreationError, DeletionError, UpdateError, ValidationError, SchemaViolation, InvalidDocument)


__all__ = ["DocumentStore", "Document", "Edge"]


class DocumentStore(object):
    """
    Store all the data of a document in hierarchy of stores and handles validation.
    Does not store private information, these are in the document.
    """

    def __init__(self, collection, validators={}, initialisation_dictionary={}, patching=False, sub_store=False, validate_init=False):
        self.store = {}
        self.patch_store = {}
        self.collection = collection
        self.validators = validators
        self.validate_init = validate_init
        self.is_sub_store = sub_store
        self.sub_stores = {}
        self.patching = patching

        self.must_validate = False
        if not self.validate_init:
            self.set(initialisation_dictionary)

        for value in self.collection._validation.values():
            if value:
                self.must_validate = True
                break

        if self.validate_init:
            self.set(initialisation_dictionary)
        
        self.patching = True

    def reset_patch(self):
        """
        reset patches
        """
        self.patch_store = {}

    def get_patches(self):
        """
        Get patches as a dictionary
        """
        if not self.must_validate:
            return self.get_store()

        result = {}
        result.update(self.patch_store)
        for k, v in self.sub_stores.items():
            result[k] = v.get_patches()
        
        return result
        
    def get_store(self):
        """
        Get the inner store as dictionary
        """
        res = {}
        res.update(self.store)
        for k, v in self.sub_stores.items():
            res[k] = v.get_store()
        
        return res

    def validateField(self, field):
        """
        Validatie a field
        """
        if field not in self.validators and not self.collection._validation['allow_foreign_fields']:
            raise SchemaViolation(self.collection.__class__, field)

        if field in self.store:
            if isinstance(self.store[field], DocumentStore):
                return self[field].validate()
            
            if field in self.patch_store:
                return self.validators[field].validate(self.patch_store[field])
            else:
                try:
                    return self.validators[field].validate(self.store[field])
                except ValidationError as e:
                    raise ValidationError( "'%s' -> %s" % ( field, str(e)) )
                except AttributeError:
                    if isinstance(self.validators[field], dict) and not isinstance(self.store[field], dict):
                        raise ValueError("Validator expected a sub document for field '%s', got '%s' instead" % (field, self.store[field]) )
                    else:
                        raise
        return True

    def validate(self):
        """
        Validate the whole document
        """
        if not self.must_validate:
            return True

        res = {}
        for field in self.validators.keys():
            try:
                if isinstance(self.validators[field], dict) and field not in self.store:
                    self.store[field] = DocumentStore(self.collection, validators = self.validators[field], initialisation_dictionary = {}, sub_store=True, validate_initialisation=self.validate_initialisation)
                self.validateField(field)
            except InvalidDocument as e:
                res.update(e.errors)
            except (ValidationError, SchemaViolation) as e:
                res[field] = str(e)

        if len(res) > 0:
            raise InvalidDocument(res)
        
        return True

    def set(self, dct):
        """
        Set the store using a dictionary
        """
        # if not self.must_validate:
        #     self.store = dct
        #     self.patch_store = dct
        #     return

        for field, value in dct.items():
            if field not in self.collection.arango_privates:
                if isinstance(value, dict):
                    if field in self.validators and isinstance(self.validators[field], dict):
                        vals = self.validators[field]
                    else:
                        vals = {}
                    self[field] = DocumentStore(self.collection, validators = vals, initialisation_dictionary = value, patch = self.patching, sub_store=True, validate_initialisation=self.validate_initialisation)
                    self.sub_stores[field] = self.store[field]
                else:
                    self[field] = value

    def __getitem__(self, field):
        """
        Get an element from the store
        """
        if (field in self.validators) and isinstance(self.validators[field], dict) and (field not in self.store):
            self.store[field] = DocumentStore(self.collection, validators = self.validators[field], initialisation_dictionary = {}, patch = self.patching, sub_store=True, validate_initialisation=self.validate_initialisation)
            self.sub_stores[field] = self.store[field]
            self.patch_store[field] = self.store[field]

        if self.collection._validation['allow_foreign_fields'] or self.collection.has_field(field):
            return self.store.get(field)

        try:
            return self.store[field]
        except KeyError:
            raise SchemaViolation(self.collection.__class__, field)

    def __setitem__(self, field, value):
        """
        Set an element in the store
        """
        if (not self.collection._validation['allow_foreign_fields']) and (field not in self.validators) and (field not in self.collection.arango_privates):
            raise SchemaViolation(self.collection.__class__, field)
        
        if field in self.collection.arango_privates:
            raise ValueError("DocumentStore cannot contain private field (got %s)" % field)

        if isinstance(value, dict):
            if field in self.validators and isinstance(self.validators[field], dict):
                vals = self.validators[field]
            else:
                vals = {}
            self.store[field] = DocumentStore(self.collection, validators = vals, initialisation_dictionary = value, patch = self.patching, sub_store=True, validate_initialisation=self.validate_initialisation)
            
            self.sub_stores[field] = self.store[field]
        else:
            self.store[field] = value

        if self.patching:
            self.patch_store[field] = self.store[field]

        if self.must_validate and self.collection._validation['on_set']:
            self.validateField(field)

    def __delitem__(self, k):
        """
        removes an element from the store
        """
        try:
            del(self.store[k])
        except:
            pass
    
        try:
            del(self.patch_store[k])
        except:
            pass

    def __contains__(self, k):
        """
        Returns true or false weither the store has a key k
        """
        return (k in self.store) or (k in self.validators)

    def __repr__(self):
        return "<store: %s>" % repr(self.store)

class Document(object):
    """
    The class that represents a document. Documents are meant to be instanciated by collections
    """

    def __init__(self, collection, json_field_init = None):
        if json_field_init is None:
            json_field_init = {}
        self.privates = ["_id", "_key", "_rev"]
        self.reset(collection, json_field_init)
        self.type_name = "ArangoDoc"

    def reset(self, collection, json_field_init = None):
        if not json_field_init:
            json_field_init = {}
        """
        Replaces the current values in the document by those in json_field_init
        """
        self.collection = collection
        self.connection = self.collection.connection
        self.documentsURL = self.collection.documentsURL


        self.URL = None
        self.set_privates(json_field_init)
        self._store = DocumentStore(self.collection, validators=self.collection._fields, initialisation_dictionary=json_field_init)
        if self.collection._validation['on_load']:
            self.validate()

        self.modified = True

    def validate(self):
        """
        validate the document
        """
        self._store.validate()
        for pField in self.collection.arango_privates:
            self.collection.validate_private(pField, getattr(self, pField))

    def set_privates(self, field_dict):
        """
        will set self._id, self._rev and self._key field.
        """
        
        for private in self.privates:
            if private in field_dict:
                setattr(self, private, field_dict[private])
            else:
                setattr(self, private, None)
        
        if self._id is not None:
            self.URL = "%s/%s" % (self.documentsURL, self._id)

    def set(self, field_dict):
        """
        set the document with a dictionary
        """
        self._store.set(field_dict)

    def save(self, wait_for_sync = False, **document_args):
        """
        Saves the document to the database by either performing a POST (for a new document) or a PUT (complete document overwrite).
        If you want to only update the modified fields use the .patch() function.
        Use document_args to put things such as 'wait_for_sync = True' (for a full list cf ArangoDB's doc).
        It will only trigger a saving of the document if it has been modified since the last save. If you want to force the saving you can use force_save()
        """
        payload = self._store.get_store()
        self._save(payload, wait_for_sync = False, **document_args)

    def _save(self, payload, wait_for_sync = False, **document_args):

        if self.modified:

            params = dict(document_args)
            params.update({'collection': self.collection.name, "wait_for_sync": wait_for_sync })

            if self.collection._validation['on_save']:
                self.validate()
            if self.URL is None:
                if self._key is not None:
                    payload["_key"] = self._key
                payload = json.dumps(payload, default=str)
                response = self.connection.session.post(self.documentsURL, params = params, data = payload)
                update = False
                data = r.json()
                self.set_privates(data)
            else:
                payload = json.dumps(payload, default=str)
                response = self.connection.session.put(self.URL, params = params, data = payload)
                update = True
                data = response.json()


            if (response.status_code == 201 or response.status_code == 202) and "error" not in data:
                if update:
                    self._rev = data['_rev']
                else:
                    self.set(data)
            else:
                if update:
                    raise UpdateError(data['errorMessage'], data)
                else:
                    raise CreationError(data['errorMessage'], data)

            self.modified = False

        self._store.reset_patch()

    def force_save(self, **document_args):
        """
        saves even if the document has not been modified since the last save
        """
        self.modified = True
        self.save(**document_args)

    def save_copy(self):
        """
        saves a copy of the object and become that copy. returns a tuple (old _key, new _key)
        """
        old_key = self._key
        self.reset(self.collection)
        self.save()
        return (old_key, self._key)

    def patch(self, keepNull = True, **document_args):
        """
        Saves the document by only updating the modified fields.
        The default behaviour concening the keepNull parameter is the opposite of ArangoDB's default, Null values won't be ignored
        Use document_args for things such as wait_for_sync = True
        """

        if self.URL is None:
            raise ValueError("Cannot patch a document that was not previously saved")

        payload = self._store.get_patches()
        
        if self.collection._validation['on_save']:
            self.validate()

        if len(payload) > 0:
            params = dict(document_args)
            params.update({'collection': self.collection.name, 'keepNull': keepNull})
            payload = json.dumps(payload, default=str)

            response = self.connection.session.patch(self.URL, params = params, data = payload)
            data = response.json()
            if (response.status_code == 201 or response.status_code == 202) and "error" not in data:
                self._rev = data['_rev']
            else:
                raise UpdateError(data['errorMessage'], data)

            self.modified = False

        self._store.reset_patch()

    def delete(self):
        """
        deletes the document from the database
        """
        if self.URL is None:
            raise DeletionError("Can't delete a document that was not saved")
        response = self.connection.session.delete(self.URL)
        data = response.json()

        if (response.status_code != 200 and response.status_code != 202) or 'error' in data:
            raise DeletionError(data['errorMessage'], data)
        self.reset(self.collection)

        self.modified = True

    def get_in_edges(self, edges, raw_results = False):
        """
        An alias for get_edges() that returns only the in Edges
        """
        return self.get_edges(edges, in_edges = True, out_edges = False, raw_results = raw_results)

    def get_out_edges(self, edges, raw_results = False):
        """
        An alias for get_edges() that returns only the out Edges
        """
        return self.get_edges(edges, in_edges = False, out_edges = True, raw_results = raw_results)

    def get_edges(self, edges, in_edges = True, out_edges = True, raw_results = False):
        """
        returns in, out, or both edges linked to self belonging the collection 'edges'.
        If raw_results a arango results will be return as fetched, if false, 
        will return a liste of Edge objects
        """
        try:
            return edges.get_edges(self, in_edges, out_edges, raw_results)
        except AttributeError:
            raise AttributeError("%s does not seem to be a valid Edges object" % edges)

    def get_store(self):
        """
        return the store in a dict format
        """
        store = self._store.get_store()
        for private in self.privates:
            v = getattr(self, private)
            if v:
                store[private] = v
        return store

    def get_patches(self):
        """
        return the patches in a dict format
        """
        return self._store.get_patches()
        """
        get an element from the document
        """
        if k in self.collection.arango_privates:
            return getattr(self, k)
        return self._store[k]

    def __setitem__(self, k, v):
        """
        set an element in the document
        """
        if k in self.collection.arango_privates:
            setattr(self, k, v)
        else:
            self._store[k] = v

    def __delitem__(self, k):
        """
        removes an element from the document
        """
        del(self._store[k])
    
    def __str__(self):
        return repr(self)

    def __repr__(self):
        private_string = []
        for p in self.collection.arango_privates:
            private_string.append("%s: %s" % (p, getattr(self, p)) )

        private_string = ', '.join(private_string)
        return "%s '%s': %s" % (self.type_name, private_string, repr(self._store))


class Edge(Document):
    """
    An Edge document
    """
    def __init__(self, edge_collection, json_field_init = None):
        if not json_field_init:
            json_field_init = {}
            
        self.type_name = "ArangoEdge"
        self.privates = ["_id", "_key", "_rev", "_from", "_to"]
        self.reset(edge_collection, json_field_init)

    def reset(self, edge_collection, json_field_init = None):
        if json_field_init is None:
            json_field_init = {}
        Document.reset(self, edge_collection, json_field_init)

    def links(self, from_vertice, to_vertice, **edge_args):
        """
        An alias to save that updates the _from and _to attributes.
        from_vertice and to_vertice, can be either strings or documents. It they are unsaved documents, they will be automatically saved.
        """
        if isinstance(from_vertice, Document) or isinstance(getattr(from_vertice, 'document', None), Document):
            if not from_vertice._id:
                from_vertice.save()
            self._from = from_vertice._id
        elif (type(from_vertice) is bytes) or (type(from_vertice) is str):
            self._from  = from_vertice
        elif not self._from:
            raise CreationError('from_vertice %s is invalid!' % str(from_vertice))
        
        if isinstance(to_vertice, Document) or isinstance(getattr(to_vertice, 'document', None), Document):
            if not to_vertice._id:
                to_vertice.save()
            self._to = to_vertice._id
        elif (type(to_vertice) is bytes) or (type(to_vertice) is str):
            self._to = to_vertice
        elif not self._to:
            raise CreationError('to_vertice %s is invalid!' % str(to_vertice))
        
        self.save(**edge_args)

    def save(self, **edge_args):
        """
        Works like Document's except that you must 
        specify '_from' and '_to' vertices before.
        There's also a links() function especially for first saves.
        """

        print("SAVINGG\n\n\n")

        if not getattr(self, "_from") or not getattr(self, "_to"):
            raise AttributeError(
                    "You must specify '_from' and '_to' attributes"
                    "before saving. You can also use the function 'links()'"
                    )

        payload = self._store.get_store()
        payload["_from"] = self._from
        payload["_to"] = self._to
        Document._save(self, payload, **edge_args)

    def __getattr__(self, k):
        if k == "_from" or k == "_to":
            return self._store[k]
        else:
            return Document.__getattr__(self, k)
