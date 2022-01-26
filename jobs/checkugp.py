# Make sure nothing is in our UpdateGraphProcessor, because extraneous things can negatively affect our runtime.
UpdateGraphProcessor = jpy.get_type("io.deephaven.engine.updategraph.UpdateGraphProcessor")
sourceCount = UpdateGraphProcessor.DEFAULT.sourceCount()

# There are some default performance logs that are going to be in here.
if sourceCount > 6:
    raise Exception("UpdateGraphProcessor has %d sources" % sourceCount)