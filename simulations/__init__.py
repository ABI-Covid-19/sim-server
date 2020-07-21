from .celeryapp import app

app.conf.update({
    'imports': ['simulations.transportation']
})
