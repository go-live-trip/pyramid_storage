# -*- coding: utf-8 -*-

import os
import mimetypes
import tempfile
import uuid

from zope.interface import implementer

from . import utils
from .exceptions import FileNotAllowed
from .interfaces import IFileStorage
from .registry import register_file_storage_impl

from pyramid_storage.s3 import S3FileStorage

import logging
log = logging.getLogger(__name__)


def includeme(config):

    impl = S3V4FileStorage.from_settings(
        config.registry.settings, prefix='storage.'
    )

    register_file_storage_impl(config, impl)


@implementer(IFileStorage)
class S3V4FileStorage(S3FileStorage):

    @classmethod
    def from_settings(cls, settings, prefix):
        options = (
            ('aws.bucket_name', True, None),
            ('aws.acl', False, 'public-read'),
            ('base_path', True, None),
            ('base_url', False, ''),
            ('extensions', False, 'default'),
            # S3 Connection options.
            ('aws.access_key', False, None),
            ('aws.secret_key', False, None),
            ('aws.use_path_style', False, False),
            ('aws.is_secure', False, True),
            ('aws.host', False, None),
            ('aws.port', False, None),
            ('aws.region', False, None),
            ('aws.num_retries', False, 1),
            ('aws.timeout', False, 5),
            ('aws.signature_version', False, 's3v4'),
        )
        kwargs = utils.read_settings(settings, options, prefix)
        kwargs = dict([(k.replace('aws.', ''), v) for k, v in kwargs.items()])
        kwargs['aws_access_key_id'] = kwargs.pop('access_key')
        kwargs['aws_secret_access_key'] = kwargs.pop('secret_key')
        return cls(**kwargs)


    def get_resource(self):

        try:
            import boto3
        except ImportError:
            raise RuntimeError("Для использования s3 у вас должен быть установлен boto3")

        from botocore.client import Config
        

        options = self.conn_options.copy()

        if options['port']:
            options['port'] = int(options['port'])
        else:
            del options['port']

        if not options['host']:
            del options['host']

        endpoint_url = '{}:{}'.format(options['host'],
            options['port']) if 'host' in options and 'port' in options else None

        config = Config(signature_version=options['signature_version']) if 'signature_version' in options else None

        session = boto3.session.Session()
        resource = session.client(
            endpoint_url=endpoint_url,
            aws_access_key_id=options['aws_access_key_id'],
            aws_secret_access_key=options['aws_secret_access_key'],
            config=config,
            region_name=options['region'],
            service_name='s3',
        )

        return resource


    def list_objects(self, folder):
        # Получить список объектов в бакете
        keys = []
        response = self.get_resource().list_objects_v2(Bucket=self.bucket_name, Prefix=folder)
        
        if 'Contents' in response:
            for key in response['Contents']:
                keys.append(key['Key'])

        return keys


    def directory(self, folder):
        """ каталог: /apple/ """
        self.get_resource().put_object(Bucket=self.bucket_name, Body='', Key=f"{folder}/")
        return f"{folder}/"


    def delete(self, filename):
        """
        Удаляет имя файла. Имя файла определяется
        как абсолютный путь, основанный на base_path. Если файл не существует,
        возвращает значение **False**, в противном случае **True**

        ::параметр filename: базовое имя файла
        """
        self.get_resource().delete_object(Bucket=self.bucket_name, Key=filename)


    def delete_objects(self, folder):
        """
        ::параметр folder Удаляет имя объекты файла.
        """
        s3 = self.get_resource()
        response = s3.list_objects_v2(Bucket=self.bucket_name, Prefix=folder)
        if 'Contents' in response:
            for key in response['Contents']:
                s3.delete_object(Bucket=self.bucket_name, Key=key['Key'])
            return True
        else:
            return False


    def exists(self, filename):
        """
        Проверьте, существует ли файл
        :параметр filename:
        :return:
        """
        response = self.get_resource().list_objects_v2(Bucket=self.bucket_name, Prefix=filename)

        if 'Contents' in response:
            for key in response['Contents']:
                return {key['Key']}
        return False


    def save(self, fs, folder=None, randomize=False, extensions=None):
        """
        :параметр fs: локальное имя файла
        :параметр folder: относительный путь к подпапке
        :параметр randomize: рандомизация имени файла
        :параметр extensions: допустимые расширения, если они не используются по умолчанию
        :возвращает: измененное имя файла
        """

        file = fs.file
        filename = fs.filename

        folder = folder and folder + '/' or ''
        extensions = extensions or self.extensions

        if not self.filename_allowed(filename, extensions):
            raise FileNotAllowed()

        if randomize:
            filename = utils.random_filename(filename)

        self.get_resource().upload_fileobj(
            Fileobj=file,
            Bucket=self.bucket_name,
            Key=f"{folder}{filename}"
        )

        return filename


    def save_image(self, obj, folder=None, extensions='png'):
        """
        :параметр obj: **cgi.FieldStorage** объект имя файла
        :параметр folder: относительный путь к подпапке
        :параметр extensions: допустимые расширения, если они не используются по умолчанию
        :возвращает: измененное имя файла
        """

        folder = folder and folder + '/' or ''

        for img in tuple('jpg jpeg png webp'.split()):
            if extensions == img:
                break
        else:
            raise FileNotAllowed()

        filename = f"{uuid.uuid4()}.{extensions}"
        
        self.get_resource().upload_fileobj(
            Fileobj=obj,
            Bucket=self.bucket_name,
            Key=f"{folder}{filename}"
        )

        return filename


    def url(self, filename, folder=None, expiration=3600):
        """
        URL (Uniform Resource Locator) — это адрес ресурса в сети Интернет.
        """
        folder = folder and folder + '/' or ''
        try:
            response = self.get_resource().generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': f"{folder}{filename}"
                },
                ExpiresIn=expiration
            )
            return response
        except NoCredentialsError:
            print("Учетные данные недоступны.")
            return None
        except Exception as e:
            print(f"Ошибка при создании предварительно заданного URL-адреса для загрузки: {e}")
            return None

