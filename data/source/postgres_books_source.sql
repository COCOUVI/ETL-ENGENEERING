
-- PostgreSQL schema for ETL source database
-- Tables: authors, categories, books

CREATE TABLE authors (
    author_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(100)
);

CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
);

CREATE TABLE books (
    book_id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    price NUMERIC(6,2),
    rating INT,
    stock INT,
    author_id INT REFERENCES authors(author_id),
    category_id INT REFERENCES categories(category_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample data

INSERT INTO authors (name, country) VALUES
('J.K Rowling','UK'),
('George Orwell','UK'),
('Chinua Achebe','Nigeria'),
('Paulo Coelho','Brazil');

INSERT INTO categories (category_name) VALUES
('Fiction'),
('Science'),
('Programming'),
('History');

INSERT INTO books (title, price, rating, stock, author_id, category_id) VALUES
('Harry Potter', 20.50, 5, 100, 1, 1),
('1984', 15.00, 5, 80, 2, 1),
('Things Fall Apart', 12.00, 4, 50, 3, 4),
('The Alchemist', 18.00, 5, 60, 4, 1);
