Створи Folder на ПК і відкрий в VS Code його.
В Extensions, завантажуй SQLite 
В мене python 3.10.

git pull

У Powershell:
python -m venv venv
venv\Scripts\activate   

pip install -r requirements.txt

cd scripts

python 1_installs_end.py
python 3_orders_end.py

cd .. (повертає до попередньої dir)

____________
cntr shift + P / type sqlite / клікни Open Database (зявиться у лівому куті внизу)
____________
python verify_sqlite.py  
прінтить рядки з таблички

