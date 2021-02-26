#!/usr/bin/env python

__author__ = "Sebastian Volpe"
__email__ = "alumnos@inove.com.ar"
__version__ = "1.1"

import os
import csv
import sqlite3

import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

engine = sqlalchemy.create_engine("sqlite:///libreria.db")
base = declarative_base()
session = sessionmaker(bind=engine)()

from config import config

script_path = os.path.dirname(os.path.realpath(__file__))

config_path_name = os.path.join(script_path, 'config.ini')
dataset = config('dataset', config_path_name)


class Autor(base):
    __tablename__ = "autor"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    
    def __repr__(self):
        return f"Autor: {self.name}"


class Libro(base):
    __tablename__ = "libro"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    pags = Column(Integer)
    author_id = Column(Integer, ForeignKey("autor.id"))

    autor = relationship("Autor")

    def __repr__(self):
        return f"Libro: {self.title}, title {self.title}, pags {self.pags}, autor {self.autor.name}"


def create_schema():
    base.metadata.drop_all(engine)
    base.metadata.create_all(engine)


def insert_autor(autor):

    Session = sessionmaker(bind=engine)
    session = Session()

    agregar = Autor(name=autor)

    session.add(agregar)
    session.commit()


def insert_libros(title, pags, name):

    Session = sessionmaker(bind=engine)
    session = Session()

    query = session.query(Autor).filter(Autor.name == name)
    cargar = query.first()

    if cargar is None:
        
        print(f"Error a crear el libnro {title}, no existe con ese autor {name}")
        return

    book = Libro(title=title, pags=pags, autor=name)
    book.autor = cargar

    session.add(book)
    session.commit()
  


def fill():

    with open(dataset['autores']) as fi:
        data = list(csv.DictReader(fi))

        for row in data:
            insert_autor(row['autor'])


    with open(dataset['libros']) as fi:
        data = list(csv.DictReader(fi))

        for row in data:
            insert_libros(row['titulo'], int(row['cantidad_paginas']), row['autor'])


def fetch(id):
 
    Session = sessionmaker(bind=engine)
    session = Session()

    if id == 0:
        query = session.query(Libro).order_by(Libro.title.desc())

        for persona in query:
            print(persona)

    else:
        result = session.query(Libro).filter(Libro.id == id)

        for imprimir in result:
           print("Libro Buscado:",imprimir)


def search_author(valor):

    Session = sessionmaker(bind=engine)
    session = Session()

    result = session.query(Libro).join(Libro.autor).filter(Libro.title == valor)

    for data in result:
        autor = data.autor

    return autor



if __name__ == "__main__":
    # Crear DB
    create_schema()

    fill()

    # Leer filas
    fetch(0)  # Ver todo el contenido de la DB
    fetch(3)  # Ver la fila 3
    fetch(20)  # Ver la fila 20

    # Buscar autor
    print(search_author('Relato de un naufrago'))

