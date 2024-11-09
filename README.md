# rtg-automotive-frontend


Add feature lets user upload an Excel (list ) of custom_labels to update or inspect

Updating pricing, when they end, lots of edits of data

Ability to add a new supplier and a new rule and a new SF

give each function a different name (4 options)

Bulk Item Uploader for Store Database - Update, append, delete 

Stock Feed Master uploader (Deleting custom_labels in stock master and in Ebay Store)

Bulk delete of custom labels -> create csv upload for ebay for item ids -> remove from stock master

Add a new feature that actually

Some item ids are not active, function for replacing the item id when eBay updates item ids every couple of weeks Phil changes them manually.

(provide user the ability update the store but only change item ids, swapping old ones for new ones)

Just replacing price, or just replacing category. First column is current item_id then other columns are the ones that are changing.

Update function: Philip doesn't display exact quantities - update functions require store 


Commands:

uvicorn app.api.mock:app --host 0.0.0.0 --port 8000 --reload