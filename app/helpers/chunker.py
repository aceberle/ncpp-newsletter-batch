# Adapted from: https://stackoverflow.com/a/312464/3113344
def get_chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
