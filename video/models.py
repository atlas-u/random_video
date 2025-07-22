from django.db import models

class RandomVideo(models.Model):
    tid = models.BigIntegerField()
    stid = models.BigIntegerField()
    channel = models.CharField(max_length=50)
    url = models.URLField()
    t1 = models.FloatField()
    t2 = models.FloatField()
    code = models.IntegerField()
    msec = models.IntegerField()
    hex = models.CharField(max_length=128)
    b64 = models.CharField(max_length=128)
    rand = models.BigIntegerField()

    def __str__(self):
        return f"Post {self.tid} - {self.channel}"
