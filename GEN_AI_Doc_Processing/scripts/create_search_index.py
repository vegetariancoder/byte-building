from backend.app.integrations.search_index import create_search_index

index = create_search_index()

print("Index created:", index.name)