from fastapi import Security, Depends, FastAPI, HTTPException, status
from fastapi.security.api_key import APIKeyHeader, APIKey

API_KEY = "hello_datascientest"
API_KEY_NAME = "access_token"

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

api = FastAPI()


async def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header == API_KEY:
        return api_key_header
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Could not validate credentials"
    )

## l' API
@api.get("/hello")
def hello():
    """
    Description:
    Cette route renvoie un message de salutation "Hello World !".

    Args:
    Aucun argument requis.

    Returns:
    - str: Le message "Hello World !".

    Raises:
    Aucune exception n'est levée.
    """
    return 'Hello World !'

@api.get("/secure")
async def secure(api_key_header: APIKey = Depends(get_api_key)):
    """
    Description:
    Cette route renvoie un message uniquement si une clé API valide est fournie dans l'en-tête.

    Args:
    - api_key_header (APIKey, dépendance): La clé API fournie dans l'en-tête de la requête.

    Returns:
    - str: Le message "Hello this is secure ! " si la clé API est valide.

    Raises:
    - HTTPException(401, detail="Unauthorized"): Si la clé API est invalide ou manquante, une exception HTTP 401 Unauthorized est levée.
    """
    return 'Hello this is secure ! '

##################################################################
# if __name__ == "__main__":
#     api.run(host="0.0.0.0",debug=True)
