"""
SQLite Database Utilities using SQLAlchemy
Contains the most commonly used database operations
For use with LLM agents
"""

from typing import List, Optional, Dict, Any
from sqlalchemy import create_engine, text, or_, and_
from sqlalchemy.orm import declarative_base, Session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from dotenv import load_dotenv
load_dotenv(override=True)

class DatabaseManager:
    """Manager class for all database operations with SQLAlchemy"""

    def __init__(self, database_url: str, base=None):
        """
        Initialize database manager
        
        Args:
            database_url: SQLite database URL (default: ./database.db)
            base: Custom declarative base object. If None, creates a new one.
                  This allows different DatabaseManager instances to use different bases.
        """
        self.database_url = database_url
        self.base = base if base is not None else declarative_base()
        self.engine = None
        self.SessionLocal = None
        self._initialize_engine()

    def _initialize_engine(self):
        """Initialize SQLAlchemy engine and session factory"""
        try:
            self.engine = create_engine(
                self.database_url,
                connect_args={"check_same_thread": False},
                echo=False  # Set to True for SQL query logging
            )
            self.SessionLocal = sessionmaker(bind=self.engine)
            print(f"Database engine initialized: {self.database_url}")
        except Exception as e:
            print(f"Error initializing database engine: {e}")
            raise

    def create_tables(self, table_names: List[str] = None) -> Dict[str, Any]:
        """
        Create tables based on table names
        
        Args:
            table_names: List of table names to create. 
                        If None, creates all tables.
                   
        Returns:
            Response dict with status and message
            
        Examples:
            # Create all tables
            db.create_tables()
            
            # Create specific tables only
            db.create_tables(['users', 'products'])
            
            # Create a single table
            db.create_tables(['users'])
        """
        try:
            if table_names is None:
                # Create all tables
                self.base.metadata.create_all(self.engine)
                return self._response("success", "All tables created successfully")
            else:
                # Normalize to list
                if isinstance(table_names, str):
                    table_names = [table_names]
                
                # Create tables for specific names only
                warnings = []
                for table_name in table_names:
                    if table_name in self.base.metadata.tables:
                        self.base.metadata.tables[table_name].create(self.engine, checkfirst=True)
                    else:
                        warnings.append(f"Table '{table_name}' not found in metadata")
                
                msg = f"Tables created successfully: {', '.join(table_names)}"
                if warnings:
                    msg += f". Warnings: {'; '.join(warnings)}"
                return self._response("success", msg, {"tables": table_names})
        except SQLAlchemyError as e:
            return self._response("error", f"Error creating tables: {str(e)}")

    def drop_tables(self, table_names: List[str] = None) -> Dict[str, Any]:
        """
        Drop tables (use with caution)
        
        Args:
            table_names: List of table names to drop.
                        If None, drops all tables.
                   
        Returns:
            Response dict with status and message
            
        Examples:
            # Drop all tables
            db.drop_tables()
            
            # Drop specific tables only
            db.drop_tables(['users', 'products'])
            
            # Drop a single table
            db.drop_tables(['users'])
        """
        try:
            if table_names is None:
                # Drop all tables
                self.base.metadata.drop_all(self.engine)
                return self._response("success", "All tables dropped successfully")
            else:
                # Normalize to list
                if isinstance(table_names, str):
                    table_names = [table_names]
                
                # Drop tables for specific names only
                warnings = []
                for table_name in table_names:
                    if table_name in self.base.metadata.tables:
                        self.base.metadata.tables[table_name].drop(self.engine, checkfirst=True)
                    else:
                        warnings.append(f"Table '{table_name}' not found in metadata")
                
                msg = f"Tables dropped successfully: {', '.join(table_names)}"
                if warnings:
                    msg += f". Warnings: {'; '.join(warnings)}"
                return self._response("success", msg, {"tables": table_names})
        except SQLAlchemyError as e:
            return self._response("error", f"Error dropping tables: {str(e)}")

    def get_model_by_table_name(self, table_name: str) -> Optional[type]:
        """
        Get a model class by its table name
        
        Args:
            table_name: Name of the table
            
        Returns:
            Model class if found, None otherwise
            
        Examples:
            # Get User model by table name
            User = db.get_model_by_table_name('users')
            
            # Get Product model
            Product = db.get_model_by_table_name('products')
            
            # Use the returned model for operations
            if User:
                users = db.read_all(User)
        """
        try:
            for model in self.base.registry.mappers:
                model_class = model.class_
                if hasattr(model_class, '__tablename__') and model_class.__tablename__ == table_name:
                    return model_class
            return None
        except Exception:
            return None

    def get_session(self) -> Session:
        """Get a new database session"""
        return self.SessionLocal()
    
    def _response(self, status: str, message: str, data: Any = None) -> Dict[str, Any]:
        """Create a standardized response dictionary for LLM compatibility"""
        return {
            "status": status,
            "message": message,
            "data": data
        }

    # ==================== CREATE Operations ====================

    def create(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create and insert a single record
        
        Args:
            table_name: Name of the table
            data: Dictionary with column names and values for the new record
            
        Returns:
            Response dict with status, message, and record data
            
        Examples:
            result = db.create('users', {'username': 'john', 'email': 'john@example.com', 'is_active': True})
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found")
        
        session = self.get_session()
        try:
            model_instance = model(**data)
            session.add(model_instance)
            session.commit()
            session.refresh(model_instance)
            record = {col.name: getattr(model_instance, col.name) for col in model.__table__.columns}
            return self._response("success", f"Record created successfully in {table_name}", record)
        except IntegrityError as e:
            session.rollback()
            return self._response("error", f"Integrity error: {str(e)}")
        except SQLAlchemyError as e:
            session.rollback()
            return self._response("error", f"Error creating record: {str(e)}")
        finally:
            session.close()

    def create_bulk(self, table_name: str, records: List[dict]) -> Dict[str, Any]:
        """
        Create multiple records in bulk
        
        Args:
            table_name: Name of the table
            records: List of dictionaries with column names and values
            
        Returns:
            Response dict with status, message, and count of created records
            
        Examples:
            records = [
                {'username': 'john', 'email': 'john@example.com'},
                {'username': 'jane', 'email': 'jane@example.com'}
            ]
            db.create_bulk('users', records)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found")
        
        session = self.get_session()
        try:
            model_instances = [model(**record) for record in records]
            session.add_all(model_instances)
            session.commit()
            count = len(model_instances)
            return self._response("success", f"{count} records created successfully in {table_name}", {"count": count})
        except IntegrityError as e:
            session.rollback()
            return self._response("error", f"Integrity error: {str(e)}")
        except SQLAlchemyError as e:
            session.rollback()
            return self._response("error", f"Error creating bulk records: {str(e)}")
        finally:
            session.close()

    # ==================== READ Operations ====================

    def read_by_id(self, table_name: str, record_id: int) -> Dict[str, Any]:
        """
        Read a record by ID
        
        Args:
            table_name: Name of the table
            record_id: ID of the record
            
        Returns:
            Response dict with status, message, and record data
            
        Examples:
            user = db.read_by_id('users', 1)
            product = db.read_by_id('products', 5)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found")
        
        session = self.get_session()
        try:
            result = session.query(model).filter(model.id == record_id).first()
            if result:
                record = {col.name: getattr(result, col.name) for col in model.__table__.columns}
                return self._response("success", f"Record found", record)
            return self._response("error", f"Record with ID {record_id} not found")
        except SQLAlchemyError as e:
            return self._response("error", f"Error reading record: {str(e)}")
        finally:
            session.close()

    def read_all(self,
        table_name: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Read all records with optional pagination
        
        Args:
            table_name: Name of the table
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            Response dict with status, message, and list of records
            
        Examples:
            records = db.read_all('users', limit=10)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found", {"records": []})
        
        session = self.get_session()
        try:
            query = session.query(model).offset(offset)
            if limit:
                query = query.limit(limit)
            results = query.all()
            records = [{col.name: getattr(r, col.name) for col in model.__table__.columns} for r in results]
            return self._response("success", f"Retrieved {len(records)} records", {"records": records, "count": len(records)})
        except SQLAlchemyError as e:
            return self._response("error", f"Error reading all records: {str(e)}", {"records": []})
        finally:
            session.close()

    def read_with_filter(self,
        table_name: str,
        filters: Dict[str, Any],
        use_or: bool = False
    ) -> Dict[str, Any]:
        """
        Read records with multiple filter conditions(matches exact column string)
        
        Args:
            table_name: Name of the table
            filters: Dictionary of column names and values for filtering
            use_or: If True, use OR logic; if False (default), use AND logic
            
        Returns:
            Response dict with status, message, and list of records
            
        Examples:
            # AND logic (default): is_active=True AND is_admin=True
            db.read_with_filter('users', {'is_active': True, 'is_admin': True})
            
            # OR logic: is_active=True OR is_admin=True
            db.read_with_filter('users', {'is_active': True, 'is_admin': True}, use_or=True)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found", {"records": []})
        
        session = self.get_session()
        try:
            query = session.query(model)
            
            if not filters:
                results = query.all()
                records = [{col.name: getattr(r, col.name) for col in model.__table__.columns} for r in results]
                return self._response("success", f"Retrieved {len(records)} records", {"records": records, "count": len(records)})
            
            # Build filter conditions
            conditions = []
            for key, value in filters.items():
                if hasattr(model, key):
                    conditions.append(getattr(model, key) == value)
            
            # Apply filters with AND or OR logic
            if use_or and conditions:
                query = query.filter(or_(*conditions))
            elif conditions:
                query = query.filter(and_(*conditions))
            
            results = query.all()
            records = [{col.name: getattr(r, col.name) for col in model.__table__.columns} for r in results]
            return self._response("success", f"Retrieved {len(records)} records", {"records": records, "count": len(records)})
        except SQLAlchemyError as e:
            return self._response("error", f"Error reading filtered records: {str(e)}", {"records": []})
        finally:
            session.close()

    def read_with_conditions(self, table_name: str, conditions: List[tuple]) -> Dict[str, Any]:
        """
        Read records with complex filter conditions
        
        Args:
            table_name: Name of the table
            conditions: List of (column_name, operator, value) tuples
                       operators: 'eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'like', 'in'
                       
        Returns:
            Response dict with status, message, and list of records
            
        Examples:
            conditions = [
                ('age', 'gte', 18),
                ('age', 'lte', 65),
                ('email', 'like', 'gmail.com')
            ]
            results = db.read_with_conditions('users', conditions)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found", {"records": []})
        
        session = self.get_session()
        try:
            query = session.query(model)
            for col_name, operator, value in conditions:
                if hasattr(model, col_name):
                    col = getattr(model, col_name)
                    if operator == 'eq':
                        query = query.filter(col == value)
                    elif operator == 'ne':
                        query = query.filter(col != value)
                    elif operator == 'gt':
                        query = query.filter(col > value)
                    elif operator == 'gte':
                        query = query.filter(col >= value)
                    elif operator == 'lt':
                        query = query.filter(col < value)
                    elif operator == 'lte':
                        query = query.filter(col <= value)
                    elif operator == 'like':
                        query = query.filter(col.like(f"%{value}%"))
                    elif operator == 'in':
                        query = query.filter(col.in_(value))
            results = query.all()
            records = [{col.name: getattr(r, col.name) for col in model.__table__.columns} for r in results]
            return self._response("success", f"Retrieved {len(records)} records", {"records": records, "count": len(records)})
        except SQLAlchemyError as e:
            return self._response("error", f"Error reading records with conditions: {str(e)}", {"records": []})
        finally:
            session.close()

    def count(self, table_name: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Count records with optional filters
        
        Args:
            table_name: Name of the table
            filters: Dictionary of filter conditions
            
        Returns:
            Response dict with status, message, and count
            
        Examples:
            total = db.count('users')
            active = db.count('users', {'is_active': True})
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found", {"count": 0})
        
        if not filters:
            filters = {}
        
        session = self.get_session()
        try:
            query = session.query(model)
            for key, value in filters.items():
                if hasattr(model, key):
                    query = query.filter(getattr(model, key) == value)
            count = query.count()
            return self._response("success", f"Found {count} records", {"count": count})
        except SQLAlchemyError as e:
            return self._response("error", f"Error counting records: {str(e)}", {"count": 0})
        finally:
            session.close()

    def exists(self, table_name: str, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if a record exists
        
        Args:
            table_name: Name of the table
            filters: Dictionary of filter conditions
            
        Returns:
            Response dict with status, message, and exists flag
            
        Examples:
            exists = db.exists('users', {'username': 'john'})
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found", {"exists": False})
        
        session = self.get_session()
        try:
            query = session.query(model)
            for key, value in filters.items():
                if hasattr(model, key):
                    query = query.filter(getattr(model, key) == value)
            exists = session.query(query.exists()).scalar()
            msg = "Record exists" if exists else "Record not found"
            return self._response("success", msg, {"exists": bool(exists)})
        except SQLAlchemyError as e:
            return self._response("error", f"Error checking existence: {str(e)}", {"exists": False})
        finally:
            session.close()

    # ==================== UPDATE Operations ====================

    def update(self, table_name: str, record_id: int, updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update a single record by ID
        
        Args:
            table_name: Name of the table
            record_id: ID of the record to update
            updates: Dictionary of column names and values to update
            
        Returns:
            Response dict with status and message
            
        Examples:
            db.update('users', 1, {'username': 'newname', 'is_active': False})
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found")
        
        session = self.get_session()
        try:
            update_dict = {}
            for key, value in updates.items():
                if hasattr(model, key):
                    update_dict[getattr(model, key)] = value
            
            if not update_dict:
                return self._response("error", "No valid columns to update")
            
            session.query(model).filter(model.id == record_id).update(update_dict)
            session.commit()
            return self._response("success", f"Record {record_id} updated successfully in {table_name}", {"record_id": record_id})
        except SQLAlchemyError as e:
            session.rollback()
            return self._response("error", f"Error updating record: {str(e)}")
        finally:
            session.close()

    def update_by_id(self, table_name: str, record_id: int, updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update a record by ID with a dictionary of updates
        
        Args:
            table_name: Name of the table
            record_id: ID of the record to update
            updates: Dictionary of column names and new values
            
        Returns:
            Response dict with status and message
            
        Examples:
            db.update_by_id('users', 1, {'username': 'jane', 'is_active': False})
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found")
        
        session = self.get_session()
        try:
            update_dict = {}
            for key, value in updates.items():
                if hasattr(model, key):
                    update_dict[getattr(model, key)] = value
            
            if not update_dict:
                return self._response("error", "No valid columns to update")
            
            session.query(model).filter(model.id == record_id).update(update_dict)
            session.commit()
            return self._response("success", f"Record {record_id} updated successfully in {table_name}", {"record_id": record_id})
        except SQLAlchemyError as e:
            session.rollback()
            return self._response("error", f"Error updating record: {str(e)}")
        finally:
            session.close()

    def update_bulk(self, table_name: str, updates: Dict[str, Any], filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update multiple records matching filter conditions
        
        Args:
            table_name: Name of the table
            updates: Dictionary of column names and new values
            filters: Dictionary of filter conditions
            
        Returns:
            Response dict with status, message, and count of updated records
            
        Examples:
            db.update_bulk('users', {'is_active': True}, {'role': 'admin'})
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found", {"count": 0})
        
        session = self.get_session()
        try:
            query = session.query(model)
            for key, value in filters.items():
                if hasattr(model, key):
                    query = query.filter(getattr(model, key) == value)
            
            update_dict = {}
            for key, value in updates.items():
                if hasattr(model, key):
                    update_dict[getattr(model, key)] = value
            
            if not update_dict:
                return self._response("error", "No valid columns to update", {"count": 0})
            
            count = query.update(update_dict)
            session.commit()
            return self._response("success", f"{count} records updated successfully in {table_name}", {"count": count})
        except SQLAlchemyError as e:
            session.rollback()
            return self._response("error", f"Error updating records: {str(e)}", {"count": 0})
        finally:
            session.close()

    # ==================== DELETE Operations ====================

    def delete_by_id(self, table_name: str, record_id: int) -> Dict[str, Any]:
        """
        Delete a record by ID
        
        Args:
            table_name: Name of the table
            record_id: ID of the record to delete
            
        Returns:
            Response dict with status and message
            
        Examples:
            db.delete_by_id('users', 1)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found")
        
        session = self.get_session()
        try:
            session.query(model).filter(model.id == record_id).delete()
            session.commit()
            return self._response("success", f"Record {record_id} deleted successfully from {table_name}", {"record_id": record_id})
        except SQLAlchemyError as e:
            session.rollback()
            return self._response("error", f"Error deleting record: {str(e)}")
        finally:
            session.close()

    def delete_with_filter(self, table_name: str, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Delete records matching filter conditions
        
        Args:
            table_name: Name of the table
            filters: Dictionary of filter conditions
            
        Returns:
            Response dict with status, message, and count of deleted records
            
        Examples:
            deleted = db.delete_with_filter('users', {'is_active': False})
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found", {"count": 0})
        
        session = self.get_session()
        try:
            query = session.query(model)
            for key, value in filters.items():
                if hasattr(model, key):
                    query = query.filter(getattr(model, key) == value)
            count = query.delete()
            session.commit()
            return self._response("success", f"{count} records deleted successfully from {table_name}", {"count": count})
        except SQLAlchemyError as e:
            session.rollback()
            return self._response("error", f"Error deleting records: {str(e)}", {"count": 0})
        finally:
            session.close()

    def delete_all(self, table_name: str) -> Dict[str, Any]:
        """
        Delete all records from a table (use with caution)
        
        Args:
            table_name: Name of the table
            
        Returns:
            Response dict with status, message, and count of deleted records
            
        Examples:
            db.delete_all('users')
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return self._response("error", f"Model for table '{table_name}' not found", {"count": 0})
        
        session = self.get_session()
        try:
            count = session.query(model).delete()
            session.commit()
            return self._response("success", f"All {count} records deleted successfully from {table_name}", {"count": count})
        except SQLAlchemyError as e:
            session.rollback()
            return self._response("error", f"Error deleting all records: {str(e)}", {"count": 0})
        finally:
            session.close()

    # ==================== AGGREGATE Operations ====================

    def get_min(self, table_name: str, column_name: str):
        """
        Get minimum value of a column
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            
        Returns:
            Minimum value or None if error
            
        Examples:
            min_age = db.get_min('users', 'age')
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return None
        
        session = self.get_session()
        try:
            from sqlalchemy import func
            result = session.query(func.min(getattr(model, column_name))).scalar()
            return result
        except SQLAlchemyError as e:
            print(f"Error getting minimum: {e}")
            return None
        finally:
            session.close()

    def get_max(self, table_name: str, column_name: str):
        """
        Get maximum value of a column
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            
        Returns:
            Maximum value or None if error
            
        Examples:
            max_age = db.get_max('users', 'age')
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return None
        
        session = self.get_session()
        try:
            from sqlalchemy import func
            result = session.query(func.max(getattr(model, column_name))).scalar()
            return result
        except SQLAlchemyError as e:
            print(f"Error getting maximum: {e}")
            return None
        finally:
            session.close()

    def get_avg(self, table_name: str, column_name: str):
        """
        Get average value of a column
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            
        Returns:
            Average value or None if error
            
        Examples:
            avg_price = db.get_avg('products', 'price')
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return None
        
        session = self.get_session()
        try:
            from sqlalchemy import func
            result = session.query(func.avg(getattr(model, column_name))).scalar()
            return result
        except SQLAlchemyError as e:
            print(f"Error getting average: {e}")
            return None
        finally:
            session.close()

    def get_sum(self, table_name: str, column_name: str):
        """
        Get sum of a column
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            
        Returns:
            Sum value or None if error
            
        Examples:
            total = db.get_sum('orders', 'amount')
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return None
        
        session = self.get_session()
        try:
            from sqlalchemy import func
            result = session.query(func.sum(getattr(model, column_name))).scalar()
            return result
        except SQLAlchemyError as e:
            print(f"Error getting sum: {e}")
            return None
        finally:
            session.close()

    # ==================== UTILITIES ====================

    def close(self) -> Dict[str, Any]:
        """Close the database connection"""
        if self.engine:
            self.engine.dispose()
            return self._response("success", "Database connection closed")
        return self._response("error", "No active database connection")

    def health_check(self) -> Dict[str, Any]:
        """Check if database connection is healthy"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return self._response("success", "Database health check passed", {"healthy": True})
        except Exception as e:
            return self._response("error", f"Database health check failed: {str(e)}", {"healthy": False})
