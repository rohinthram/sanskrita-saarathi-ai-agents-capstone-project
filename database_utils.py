"""
SQLite Database Utilities using SQLAlchemy
Contains the most commonly used database operations
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, select, update, delete
from sqlalchemy.orm import declarative_base, Session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from datetime import datetime
from typing import List, Optional, Dict, Any
import os
from dotenv import load_dotenv
load_dotenv(override=True)

# Database configuration
# DATABASE_URL = os.getenv("DATABASE_URL")

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

    def create_tables(self, table_names: List[str] = None):
        """
        Create tables based on table names
        
        Args:
            table_names: List of table names to create. 
                        If None, creates all tables.
                   
        Returns:
            None
            
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
                print("All tables created successfully")
            else:
                # Normalize to list
                if isinstance(table_names, str):
                    table_names = [table_names]
                
                # Create tables for specific names only
                for table_name in table_names:
                    if table_name in self.base.metadata.tables:
                        self.base.metadata.tables[table_name].create(self.engine, checkfirst=True)
                    else:
                        print(f"Warning: Table '{table_name}' not found in metadata")
                
                print(f"Tables created successfully: {', '.join(table_names)}")
        except SQLAlchemyError as e:
            print(f"Error creating tables: {e}")
            raise

    def drop_tables(self, table_names: List[str] = None):
        """
        Drop tables (use with caution)
        
        Args:
            table_names: List of table names to drop.
                        If None, drops all tables.
                   
        Returns:
            None
            
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
                print("All tables dropped successfully")
            else:
                # Normalize to list
                if isinstance(table_names, str):
                    table_names = [table_names]
                
                # Drop tables for specific names only
                for table_name in table_names:
                    if table_name in self.base.metadata.tables:
                        self.base.metadata.tables[table_name].drop(self.engine, checkfirst=True)
                    else:
                        print(f"Warning: Table '{table_name}' not found in metadata")
                
                print(f"Tables dropped successfully: {', '.join(table_names)}")
        except SQLAlchemyError as e:
            print(f"Error dropping tables: {e}")
            raise

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
            
            print(f"Model for table '{table_name}' not found")
            return None
        except Exception as e:
            print(f"Error retrieving model: {e}")
            return None

    def get_session(self) -> Session:
        """Get a new database session"""
        return self.SessionLocal()

    # ==================== CREATE Operations ====================

    def create(self, table_name: str, **kwargs) -> Optional[dict]:
        """
        Create and insert a single record
        
        Args:
            table_name: Name of the table
            **kwargs: Column name and value pairs for the new record
            
        Returns:
            The created record as dictionary with ID, or None if failed
            
        Examples:
            result = db.create('users', username='john', email='john@example.com', is_active=True)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return None
        
        session = self.get_session()
        try:
            # Create instance from kwargs
            model_instance = model(**kwargs)
            session.add(model_instance)
            session.commit()
            session.refresh(model_instance)
            print(f"Record created successfully in {table_name}")
            return {col.name: getattr(model_instance, col.name) for col in model.__table__.columns}
        except IntegrityError as e:
            session.rollback()
            print(f"Integrity error: {e}")
            return None
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error creating record: {e}")
            return None
        finally:
            session.close()

    def create_bulk(self, table_name: str, records: List[dict]) -> bool:
        """
        Create multiple records in bulk
        
        Args:
            table_name: Name of the table
            records: List of dictionaries with column names and values
            
        Returns:
            True if successful, False otherwise
            
        Examples:
            records = [
                {'username': 'john', 'email': 'john@example.com'},
                {'username': 'jane', 'email': 'jane@example.com'}
            ]
            db.create_bulk('users', records)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return False
        
        session = self.get_session()
        try:
            # Create instances from dicts
            model_instances = [model(**record) for record in records]
            session.add_all(model_instances)
            session.commit()
            print(f"{len(model_instances)} records created successfully in {table_name}")
            return True
        except IntegrityError as e:
            session.rollback()
            print(f"Integrity error: {e}")
            return False
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error creating bulk records: {e}")
            return False
        finally:
            session.close()

    # ==================== READ Operations ====================

    def read_by_id(self, table_name: str, record_id: int) -> Optional[dict]:
        """
        Read a record by ID
        
        Args:
            table_name: Name of the table
            record_id: ID of the record
            
        Returns:
            Dictionary with record data, or None if not found
            
        Examples:
            user = db.read_by_id('users', 1)
            product = db.read_by_id('products', 5)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return None
        
        session = self.get_session()
        try:
            result = session.query(model).filter(model.id == record_id).first()
            if result:
                return {col.name: getattr(result, col.name) for col in model.__table__.columns}
            return None
        except SQLAlchemyError as e:
            print(f"Error reading record: {e}")
            return None
        finally:
            session.close()

    def read_all(self, table_name: str, limit: Optional[int] = None, offset: int = 0) -> List[dict]:
        """
        Read all records with optional pagination
        
        Args:
            table_name: Name of the table
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of records as dictionaries
            
        Examples:
            records = db.read_all('users', limit=10)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return []
        
        session = self.get_session()
        try:
            query = session.query(model).offset(offset)
            if limit:
                query = query.limit(limit)
            results = query.all()
            return [{col.name: getattr(r, col.name) for col in model.__table__.columns} for r in results]
        except SQLAlchemyError as e:
            print(f"Error reading all records: {e}")
            return []
        finally:
            session.close()

    def read_with_filter(self, table_name: str, use_or: bool = False, **filters) -> List[dict]:
        """
        Read records with multiple filter conditions
        
        Args:
            table_name: Name of the table
            use_or: If True, use OR logic; if False (default), use AND logic
            **filters: Column name and value pairs for filtering
            
        Returns:
            List of filtered records as dictionaries
            
        Examples:
            # AND logic (default): is_active=True AND is_admin=True
            db.read_with_filter('users', is_active=True, is_admin=True)
            
            # OR logic: is_active=True OR is_admin=True
            db.read_with_filter('users', use_or=True, is_active=True, is_admin=True)
        """
        from sqlalchemy import or_, and_
        
        model = self.get_model_by_table_name(table_name)
        if not model:
            return []
        
        session = self.get_session()
        try:
            query = session.query(model)
            
            if not filters:
                results = query.all()
                return [{col.name: getattr(r, col.name) for col in model.__table__.columns} for r in results]
            
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
            return [{col.name: getattr(r, col.name) for col in model.__table__.columns} for r in results]
        except SQLAlchemyError as e:
            print(f"Error reading filtered records: {e}")
            return []
        finally:
            session.close()

    def read_with_conditions(self, table_name: str, conditions: List[tuple]) -> List[dict]:
        """
        Read records with complex filter conditions
        
        Args:
            table_name: Name of the table
            conditions: List of (column_name, operator, value) tuples
                       operators: 'eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'like', 'in'
                       
        Returns:
            List of filtered records as dictionaries
            
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
            return []
        
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
            return [{col.name: getattr(r, col.name) for col in model.__table__.columns} for r in results]
        except SQLAlchemyError as e:
            print(f"Error reading records with conditions: {e}")
            return []
        finally:
            session.close()

    def count(self, table_name: str, **filters) -> int:
        """
        Count records with optional filters
        
        Args:
            table_name: Name of the table
            **filters: Optional filter conditions
            
        Returns:
            Number of records
            
        Examples:
            total = db.count('users')
            active = db.count('users', is_active=True)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return 0
        
        session = self.get_session()
        try:
            query = session.query(model)
            for key, value in filters.items():
                if hasattr(model, key):
                    query = query.filter(getattr(model, key) == value)
            count = query.count()
            return count
        except SQLAlchemyError as e:
            print(f"Error counting records: {e}")
            return 0
        finally:
            session.close()

    def exists(self, table_name: str, **filters) -> bool:
        """
        Check if a record exists
        
        Args:
            table_name: Name of the table
            **filters: Filter conditions
            
        Returns:
            True if record exists, False otherwise
            
        Examples:
            exists = db.exists('users', username='john')
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return False
        
        session = self.get_session()
        try:
            query = session.query(model)
            for key, value in filters.items():
                if hasattr(model, key):
                    query = query.filter(getattr(model, key) == value)
            exists = session.query(query.exists()).scalar()
            return exists
        except SQLAlchemyError as e:
            print(f"Error checking existence: {e}")
            return False
        finally:
            session.close()

    # ==================== UPDATE Operations ====================

    def update(self, table_name: str, record_id: int, **updates) -> bool:
        """
        Update a single record by ID
        
        Args:
            table_name: Name of the table
            record_id: ID of the record to update
            **updates: Column name and value pairs to update
            
        Returns:
            True if successful, False otherwise
            
        Examples:
            db.update('users', 1, username='newname', is_active=False)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return False
        
        session = self.get_session()
        try:
            # Convert column names to mapped attributes
            update_dict = {}
            for key, value in updates.items():
                if hasattr(model, key):
                    update_dict[getattr(model, key)] = value
            
            if not update_dict:
                print(f"No valid columns to update")
                return False
            
            session.query(model).filter(model.id == record_id).update(update_dict)
            session.commit()
            print(f"Record {record_id} updated successfully in {table_name}")
            return True
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error updating record: {e}")
            return False
        finally:
            session.close()

    def update_by_id(self, table_name: str, record_id: int, updates: Dict[str, Any]) -> bool:
        """
        Update a record by ID with a dictionary of updates
        
        Args:
            table_name: Name of the table
            record_id: ID of the record to update
            updates: Dictionary of column names and new values
            
        Returns:
            True if successful, False otherwise
            
        Examples:
            db.update_by_id('users', 1, {'username': 'jane', 'is_active': False})
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return False
        
        session = self.get_session()
        try:
            # Convert column names to mapped attributes
            update_dict = {}
            for key, value in updates.items():
                if hasattr(model, key):
                    update_dict[getattr(model, key)] = value
            
            if not update_dict:
                print(f"No valid columns to update")
                return False
            
            session.query(model).filter(model.id == record_id).update(update_dict)
            session.commit()
            print(f"Record {record_id} updated successfully in {table_name}")
            return True
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error updating record: {e}")
            return False
        finally:
            session.close()

    def update_bulk(self, table_name: str, updates: Dict[str, Any], **filters) -> int:
        """
        Update multiple records matching filter conditions
        
        Args:
            table_name: Name of the table
            updates: Dictionary of column names and new values
            **filters: Filter conditions
            
        Returns:
            Number of records updated
            
        Examples:
            db.update_bulk('users', {'is_active': True}, role='admin')
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return 0
        
        session = self.get_session()
        try:
            query = session.query(model)
            for key, value in filters.items():
                if hasattr(model, key):
                    query = query.filter(getattr(model, key) == value)
            
            # Convert column names to mapped attributes
            update_dict = {}
            for key, value in updates.items():
                if hasattr(model, key):
                    update_dict[getattr(model, key)] = value
            
            if not update_dict:
                print(f"No valid columns to update")
                return 0
            
            count = query.update(update_dict)
            session.commit()
            print(f"{count} records updated successfully in {table_name}")
            return count
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error updating records: {e}")
            return 0
        finally:
            session.close()

    # ==================== DELETE Operations ====================

    def delete_by_id(self, table_name: str, record_id: int) -> bool:
        """
        Delete a record by ID
        
        Args:
            table_name: Name of the table
            record_id: ID of the record to delete
            
        Returns:
            True if successful, False otherwise
            
        Examples:
            db.delete_by_id('users', 1)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return False
        
        session = self.get_session()
        try:
            session.query(model).filter(model.id == record_id).delete()
            session.commit()
            print(f"Record {record_id} deleted successfully from {table_name}")
            return True
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error deleting record: {e}")
            return False
        finally:
            session.close()

    def delete_with_filter(self, table_name: str, **filters) -> int:
        """
        Delete records matching filter conditions
        
        Args:
            table_name: Name of the table
            **filters: Filter conditions
            
        Returns:
            Number of records deleted
            
        Examples:
            deleted = db.delete_with_filter('users', is_active=False)
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return 0
        
        session = self.get_session()
        try:
            query = session.query(model)
            for key, value in filters.items():
                if hasattr(model, key):
                    query = query.filter(getattr(model, key) == value)
            count = query.delete()
            session.commit()
            print(f"{count} records deleted successfully from {table_name}")
            return count
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error deleting records: {e}")
            return 0
        finally:
            session.close()

    def delete_all(self, table_name: str) -> int:
        """
        Delete all records from a table (use with caution)
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of records deleted
            
        Examples:
            db.delete_all('users')
        """
        model = self.get_model_by_table_name(table_name)
        if not model:
            return 0
        
        session = self.get_session()
        try:
            count = session.query(model).delete()
            session.commit()
            print(f"All {count} records deleted successfully from {table_name}")
            return count
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error deleting all records: {e}")
            return 0
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

    def close(self):
        """Close the database connection"""
        if self.engine:
            self.engine.dispose()
            print("Database connection closed")

    def health_check(self) -> bool:
        """Check if database connection is healthy"""
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            print("Database health check passed")
            return True
        except Exception as e:
            print(f"Database health check failed: {e}")
            return False


# ==================== Example ORM Models ====================

# class User(Base):
#     """Example User model"""
#     __tablename__ = "users"

#     id = Column(Integer, primary_key=True)
#     username = Column(String(50), unique=True, nullable=False)
#     email = Column(String(100), unique=True, nullable=False)
#     password = Column(String(255), nullable=False)
#     is_active = Column(Boolean, default=True)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

#     def __repr__(self):
#         return f"<User(id={self.id}, username={self.username}, email={self.email})>"


# class Product(Base):
#     """Example Product model"""
#     __tablename__ = "products"

#     id = Column(Integer, primary_key=True)
#     name = Column(String(100), nullable=False)
#     description = Column(Text)
#     price = Column(Float, nullable=False)
#     quantity = Column(Integer, default=0)
#     is_available = Column(Boolean, default=True)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

#     def __repr__(self):
#         return f"<Product(id={self.id}, name={self.name}, price={self.price})>"


# class Order(Base):
#     """Example Order model"""
#     __tablename__ = "orders"

#     id = Column(Integer, primary_key=True)
#     user_id = Column(Integer, nullable=False)
#     product_id = Column(Integer, nullable=False)
#     quantity = Column(Integer, nullable=False)
#     total_price = Column(Float, nullable=False)
#     status = Column(String(50), default="pending")
#     created_at = Column(DateTime, default=datetime.utcnow)
#     updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

#     def __repr__(self):
#         return f"<Order(id={self.id}, user_id={self.user_id}, status={self.status})>"


# ==================== Initialization ====================

# You can now create DatabaseManager instances with custom Base objects:
# Example 1: Use default Base
# db = DatabaseManager()

# Example 2: Create DatabaseManager with custom Base
# custom_base = declarative_base()
# db = DatabaseManager(base=custom_base)

# Example 3: Multiple managers with different bases
# base1 = declarative_base()
# base2 = declarative_base()
# db1 = DatabaseManager('sqlite:///db1.db', base=base1)
# db2 = DatabaseManager('sqlite:///db2.db', base=base2)

# Create a default database manager instance with auto-created base
# db = DatabaseManager()

