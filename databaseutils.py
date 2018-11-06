# Set of utilities intended to access the various mgp databases
# Christopher Dane Barland

import pyodbc
from datetime import datetime, date
from datetime import timedelta
import pandas
import numpy as np
from os import path
import xlsxwriter

STONEEDGE_DB = 'C:/Stoneedge/SEOrdman.mdb'
SQL_DB = 'DRIVER={SQL Server Native Client 11.0};SERVER=CADILLAC;Trusted_Connection=yes;'
##SQL_DB = 'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+STONEEDGE_DB

def workDaysDiff(start, end):
    daysDelta = (end-start).days
    hangDays = np.sign(daysDelta)*(abs(daysDelta)) % 7
    weeks = daysDelta//7
    while hangDays >= 5:
        hangDays -= 7
        weeks += 1
    weekDaysDelta = hangDays + (5 * weeks)
    return(weekDaysDelta)


def getStationDays(row):
    curTime = datetime.now()
    scanTime = datetime.combine(row['Ordered'], datetime.min.time())
    colName = 'Status'
    colNum = 6
    if row['Engraving'] != None and row['Engraving'] > scanTime:
        scanTime = row['Engraving']
        colName = 'Engraving'
        colNum += 1
    if row['Welding'] != None and row['Welding'] > scanTime:
        scanTime = row['Welding']
        colName = 'Welding'
        colNum += 1
    if row['PC/Paint'] != None and row['PC/Paint'] > scanTime:
        scanTime = row['PC/Paint']
        colName = 'PC/Paint'
        colNum += 1
    if row['Paint Fill'] != None and row['Paint Fill'] > scanTime:
        scanTime = row['Paint Fill']
        colName = 'Paint Fill'
        colNum += 1
    if row['Packaging'] != None and row['Packaging'] > scanTime:
        scanTime = row['Packaging']
        colName = 'Packaging'
        colNum += 1
    return(workDaysDiff(scanTime.date(), curTime.date()),colName, colNum)

class Database:
    def __init__(self, user = ''):
        self.conn = pyodbc.connect(SQL_DB)
        self.cursor = self.conn.cursor()
        self.user = user

    def __enter__(self):
        return self

    def close(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
        print('Connection closed')

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def get_sku(self,orderstring):
        ordernum = int(orderstring[:-2])
        itemnum = int(orderstring[-2:])
        SQL = """
SELECT OrderNumber, ItemNumber, SKU
FROM "Order Details"
WHERE OrderNumber=? AND ItemNumber=?;
"""
        values = (ordernum, itemnum)
        self.cursor.execute(SQL,values)
        result = self.cursor.fetchone()[2]
        return(result)

    def get_customer_name(self, ordernum):
        SQL = """
SELECT OrderNumber, Company, ShipName
FROM "Orders"
WHERE OrderNumber = ?
"""
        self.cursor.execute(SQL, ordernum)
        result = self.cursor.fetchone()
        if result is None:
            return("")
        if (result[1] is None) or (result[1] == ""):
            if result[2] is None:
                name = ""
            else:
                name = result[2]
        else:
            name = result[1]
        return(name)

    def insert_note(self,note,orderstring,initials, statusstring, hasItem = True):
        datestr = datetime.strftime(datetime.today(),'%Y, %m, %d')
        sqldate = 'datetime.datetime(' + datestr + ', 0, 0)'
        timestr = datetime.strftime(datetime.now(),'%Y, %m, %d, %H, %M, %S')
        sqltime = 'datetime.datetime(' + timestr + ')'

        if hasItem:
            ordernum = orderstring[:-2]
            itemnum = orderstring[-2:]
        else:
            ordernum = orderstring
            itemnum = '00'

        sql = """
SET NOCOUNT ON;
DECLARE @DateTimeVal DATETIME;
SET @DateTimeVal = GETDATE();
INSERT INTO Notes (Type, NumericKey, ItemNumber, EntryDate, EntryTime, Notes, Completed, EnteredBy, ParentType, ParentKey, Event)
VALUES ('O', ?, ?, @DateTimeVal , @DateTimeVal, ?, 0, ?, 'O', ?, ?);
"""
        params = (ordernum, itemnum, note, initials,  str(ordernum), statusstring)
        self.cursor.execute(sql, params)

    def update_status(self,
                      statusstring,
                      orderstring,
                      initials,
                      commit = True,
                      rework=False,
                      note = ''):
        ordernum = int(orderstring[:-2])
        itemnum = int(orderstring[-2:])
        self.update_status_num(statusstring,
                               ordernum,
                               itemnum,
                               initials,
                               commit = commit,
                               rework=rework,
                               note = note)

    def update_status_num(self,
                          statusstring,
                          ordernum,
                          itemnum,
                          initials,
                          commit = True,
                          rework = False,
                          note = ''):
        if rework:
            note = "REWORK "+note
            SQL = """
UPDATE "Order Details"
SET Date1=NULL, Date2=NULL, Date3=NULL, Date4=NULL, Date5=NULL
WHERE OrderNumber=? AND ItemNumber=?;
"""
            params = (ordernum, itemnum)
            self.cursor.execute(SQL,params)

        today = datetime.today()
        ColDict = {"Engraving":", Date1=?",
                   "Welding":", Date2=?",
                   "PC/Paint":", Date3=?",
                   "Paint Fill":", Date4=?",
                   "Packaging": ", Date5=?"}
        SQL = """
UPDATE "Order Details"
SET Status=?, StatusChanged=-1{0}
WHERE OrderNumber=? AND ItemNumber=?;
"""

        try:
            SQL = SQL.format(ColDict[statusstring])
            params = (statusstring, today, ordernum, itemnum)
        except KeyError:
            SQL = SQL.format("")
            params = (statusstring, ordernum, itemnum)

        self.cursor.execute(SQL,params)
        note = 'Item '+str(itemnum)+' in '+statusstring+' '+note
        self.insert_note(note,
                         str(ordernum)+str(format(itemnum, '02')),
                         initials,
                         statusstring)
        if commit:
            self.conn.commit()

    def update_order_status(self,
                            statusstring,
                            ordernum,
                            initials,
                            commit = True,
                            rework=False,
                            note = ''):
        if type(ordernum) is str:
            ordernum = int(ordernum[:-2])
        #Retrieve list of items, SKU, and identifiers
        itemList = self.get_order_items(ordernum)

        #Tick through items in the order
        skuList = []
        for row in itemList:
            if(row.Adjustment == False):
                #Change status to PREPARING TO SHIP
                self.update_status_num(statusstring,
                                       ordernum,
                                       row.ItemNumber,
                                       initials,
                                       commit = False,
                                       rework = rework,
                                       note = note)
                skuList.append(row.SKU)

        if commit:
            self.conn.commit()
        return(skuList, itemList)

    def get_inventory_data(self):
        SQL = """
SELECT LocalSKU, ItemName, QOH, Price, Location, Discontinued, Text5, Category, Image, Price2, Price3, Price4, Price5, Price6, Price8, Price9, Price10, RetailPrice, Description, Length, Width, Height, UPC, MAP
FROM Inventory
WHERE Discontinued=0 AND QOH>=0 AND NOT UPC='None' AND NOT UPC='' AND Category='FGPN'
ORDER BY QOH DESC
"""
        self.cursor.execute(SQL)
        inventoryData = self.cursor.fetchall()
        return(inventoryData)

    def get_inventory_dict(self):
        SQL = """
SELECT LocalSKU, ItemName, QOH, Price, Location, Discontinued, Text5, Category, Image, Price2, Price3, Price4, Price5, Price6, Price7, Price8, Price9, Price10, RetailPrice, Description, Length, Width, Height, UPC, MAP
FROM Inventory
WHERE Discontinued=0 AND QOH>=0 AND NOT UPC='None' AND NOT UPC='' AND Category='FGPN'
ORDER BY QOH DESC
"""
        self.cursor.execute(SQL)
        columns = [column[0] for column in self.cursor.description]
        invDict = {}
        for row in self.cursor.fetchall():
            invDict[row.LocalSKU] = (dict(zip(columns, row)))
        return(invDict)

    def get_inventory_row(self, sku):
        SQL = """
SELECT *
FROM Inventory
WHERE LocalSKU = ?
"""
        values = sku
        self.cursor.execute(SQL,sku)
        inventoryData = self.cursor.fetchone()
        return(inventoryData)

    def get_row(self, sku):
        SQL = """
SELECT *
FROM Inventory
WHERE LocalSKU = ?
"""
        values = sku
        self.cursor.execute(SQL,sku)
        columns = [column[0] for column in self.cursor.description]

        try:
            results = (dict(zip(columns,self.cursor.fetchone())))
        except TypeError:
            print("Sku not found: " + sku)
            results = None

        return(results)

    def get_image(self, sku):
        SQL = """
SELECT LocalSKU, Image
FROM Inventory
WHERE LocalSKU = ?
"""
        values = sku
        self.cursor.execute(SQL,sku)
        imageData = self.cursor.fetchone()
        return(imageData.Image)

    def get_sku_lists(self):
        ##Returns list of active SKUs, and a list of discontinued SKUs
        SQL = """
SELECT LocalSKU, Discontinued, Category
FROM Inventory
"""
        self.cursor.execute(SQL)
        invList = self.cursor.fetchall()

        fgpnList = []
        custList = []
        discontinued = []
        for row in invList:
            if row.Discontinued or (row.Category == "MTO"):
                discontinued.append(row.LocalSKU)
            elif (row.Category == 'FGPN'):
                fgpnList.append(row.LocalSKU)
            elif (row.Category == 'Base'):
                custList.append(row.LocalSKU)

        return(fgpnList, custList, discontinued)

    def get_order_items(self, orderNumber):
        SQL = """
SELECT OrderNumber, ItemNumber, Adjustment, SKU, QuantityNeeded,
QuantityShipped, QuantityOrdered, QuantityPacked, Status
FROM "Order Details"
WHERE OrderNumber = ?
"""
        values = orderNumber
        self.cursor.execute(SQL, values)
        orderData = self.cursor.fetchall()
        return(orderData)

    def fill_backorder(self, orderNumber):
        SQL = """
UPDATE "Order Details"
SET QuantityShipped = QuantityOrdered, QuantityNeeded = 0,
Backordered = 0, DateShipped = GETDATE()
WHERE OrderNumber=? AND Adjustment = 0
"""
        values = orderNumber
        self.cursor.execute(SQL, values)
        return()

    def is_approved(self, orderNumber):
        SQL = """
SELECT OrderNumber, Approved
FROM Orders
WHERE OrderNumber = ?
"""
        self.cursor.execute(SQL, orderNumber)
        result = self.cursor.fetchone()
        return(result.Approved)

    def mark_shipped(self, orderNumber):
        initials = self.user

        #Retrieve list of items, SKU, and identifiers
        itemList = self.get_order_items(orderNumber)

        #Check for shipping adjustment.
        hasAdjustment = False
        for row in itemList:
            if(row.Adjustment and row.SKU == "Product"):
                #Delete row, switch boolean
                self.delete_item(orderNumber, row.ItemNumber)
                hasAdjustment = True
                break
        if not hasAdjustment:
            return(0,None)

        #If order is approved, execute ship stock. Defaults to ship complete
        if self.is_approved(orderNumber):
            backorderList = self.extract_backordered_items(orderNumber)
            orderNumber = self.create_new_order(backorderList, orderNumber)

        #Move quantity on backorder into quantity shipped
        self.fill_backorder(orderNumber)
        skuList = self.update_order_status("PREPARING TO SHIP!",
                                           orderNumber,
                                           initials,
                                           commit = False)
        self.conn.commit()
        return(skuList, orderNumber)

    def delete_item(self, orderNumber, itemNumber):
        SQL = """
DELETE FROM "Order Details"
WHERE OrderNumber = ? AND ItemNumber = ?
"""
        values = orderNumber, itemNumber
        self.cursor.execute(SQL, values)
        return

    def has_shipped_items(self, orderNumber):
        SQL = """
SELECT OrderNumber, QuantityShipped
FROM "Order Details"
WHERE OrderNumber=? AND QuantityShipped > 0
"""
        values = orderNumber
        self.cursor.execute(SQL, values)
        data = self.cursor.fetchone()
        if data is None:
            return(False)
        else:
            return(True)

    def extract_backordered_items(self, orderNumber):
        SQL = """
SELECT *
FROM "Order Details"
WHERE OrderNumber=? AND QuantityOrdered > QuantityShipped
"""
        values = orderNumber
        self.cursor.execute(SQL, values)
        orderedItems = self.cursor.fetchall()

        SQL = """
UPDATE "Order Details"
SET QuantityNeeded = 0,
Backordered = 0, DateShipped = GETDATE()
WHERE OrderNumber=? AND Adjustment = 0
"""
        self.cursor.execute(SQL, values)

        backorderedItems = []
        for item in orderedItems:
            if (item.QuantityOrdered > item.QuantityShipped):
                item.QuantityOrdered = item.QuantityNeeded
                item.QuantityShipped = item.QuantityNeeded
                item.QuantityNeeded = 0
                item.BilledSubtotal = (item.PricePerUnit*item.QuantityOrdered)
                item.ShippedSubtotal = item.BilledSubtotal
                item.FinalSubtotal = None
                item.DateShipped = None
                backorderedItems.append(item)

        return(backorderedItems)

    def copy_order(self, currentOrderNumber):
        SQL = """
SELECT *
FROM "Orders"
WHERE OrderNumber = ?
"""
        self.cursor.execute(SQL,currentOrderNumber)
        row = self.cursor.fetchone()

        SQL = """
SELECT TOP(1) [OrderNumber]
FROM "Orders"
ORDER BY (OrderNumber) DESC
"""
        self.cursor.execute(SQL)
        bottomOrderNumber = self.cursor.fetchone()
        newOrderNumber = bottomOrderNumber.OrderNumber+1

        row.OrderNumber = newOrderNumber
        row.GrandTotal = 0.0
        row.ProductTotal = 0.0
        row.NumItems = 0
        row.FinalProductTotal = 0.0
        row.FinalGrandTotal = 0.0
        row.BackOrdersToFill = False
        row.ShippedWeight = 0
        row.ExpectedNet = 0.0
        row.ActualNet = 0.0
        row.TaxTotal = 0.0
        row.ShippingTotal = 0
        row.Approved = False
        row.OrderDate = datetime.now()
        row.OrderTime = datetime.now()
        row.DateCreated = datetime.now()
        row.SourceOrderNumber = currentOrderNumber

        noteString = "Copied from Order #"+str(currentOrderNumber)
        self.insert_note(noteString, str(newOrderNumber), 'Shipping Dept', 'Order Filled' ,hasItem=False)
        noteString = "Copied to Order #"+str(newOrderNumber)
        self.insert_note(noteString, str(currentOrderNumber), 'Shipping Dept', 'Order Filled', hasItem=False)

        return(newOrderNumber, row)

    def insert_row(self, item, table = '"Order Details"'):
        SQL = """
INSERT INTO {0}
VALUES """.format(table)

        values = []
        for i in range(0,len(item)-1):
            SQL += "?, "
            values.append(item[i])

        SQL += "DEFAULT)"
        self.cursor.execute(SQL,values)
        return

    def insert_rows(self, itemList):
        for item in itemList:
            self.insert_row(item)
        return

    def create_new_order(self, itemList, orderNumber):
        newOrderNumber, row = self.copy_order(orderNumber)
        self.insert_row(row, table = "Orders")
        finalTotal = 0.0
        finalWeight = 0.0
        actualNet = 0.0
        for item in itemList:
            item.OrderNumber = newOrderNumber
            item.DetailDate = datetime.today()
            self.insert_row(item)
            print(item)
            finalTotal += float(item.BilledSubtotal)
            finalWeight += float(item.ActualWeight+item.QuantityShipped)
            actualNet -= abs(float(item.CostPerUnit*item.QuantityOrdered))
        expectedNet = finalTotal+actualNet

        SQL = """
UPDATE Orders
SET BalanceDue = ?, FinalProductTotal = BalanceDue, FinalGrandTotal = BalanceDue,
ShippedWeight = ?,
ExpectedNet = ?, ActualNet = ?
WHERE OrderNumber = ?
"""
        values = (finalTotal, finalWeight, expectedNet, actualNet, newOrderNumber)
        self.cursor.execute(SQL, values)
        return(newOrderNumber)

    def update_inventory(self, sku, valueDict):
        SQL1 = 'UPDATE "Inventory"\n'
        SQL3 = '\nWHERE LocalSKU=?'
        SQL2 = "SET "
        params = []

        for key, item in valueDict.items():
            SQL2+= key + "=?, "
            params.append(item)
        params.append(sku)
        SQL2 = SQL2[:-2]
        SQL = SQL1+SQL2+SQL3+';'
        self.cursor.execute(SQL,params)

    def get_status_report(self, statusList = None, filepath = "", filename = "StatusReport.xlsx"):
        #Create Status Report DataFrame
        report = pandas.DataFrame()
        if statusList is not None:
            SQL = """
SELECT SKU, Status, OrderNumber, ItemNumber, ExpectedShipDate, DetailDate, QuantityNeeded, Date1, Date2, Date3, Date4, Date5
FROM "Order Details"
WHERE Status = {0}
AND QuantityNeeded > 0
"""
        else:
            SQL = """
SELECT SKU, Status, FinalSubtotal, OrderNumber, ItemNumber, ExpectedShipDate, DetailDate, QuantityNeeded, Date1, Date2, Date3, Date4, Date5
FROM "Order Details"
WHERE QuantityNeeded > 0{0}
"""
            statusList=[""]
        SQL2 = """
SELECT NumericKey, ItemNumber, EntryDate
FROM "Notes"
WHERE NumericKey = ? AND ItemNumber = ?
"""
        OrderList = []
        LateOrderList = []
        NumSets = 0
        NumLateSets = 0
        for searchItem in statusList:
            param = searchItem
            self.cursor.execute(SQL.format(searchItem))
            data = self.cursor.fetchall()
            dataRow = pandas.Series()

            i=0
            for row in data:

                ##OrderList is used for the Summary
                OrderList.append(row.OrderNumber)

                params = (row.OrderNumber, row.ItemNumber)
                self.cursor.execute(SQL2, params)
                rowTime = self.cursor.fetchone()

                if rowTime is None:
                    date = ''

                else:
                    date = rowTime.EntryDate

                if row.ExpectedShipDate is not None:
                    daysLeft = workDaysDiff(datetime.today().date(),
                                            row.ExpectedShipDate.date())
                    if daysLeft < 0:
                        LateOrderList.append(row.OrderNumber)
                        NumLateSets += row.QuantityNeeded
                else:
                    daysLeft = None

                if row.ExpectedShipDate == '' or row.ExpectedShipDate == None:
                    daysLeft = -99

                dataRow = pandas.Series({'SKU':row.SKU,
                                         'Sets': row.QuantityNeeded,
                                         'Status':row.Status,
                                         'Days': daysLeft,
                                         'Engraving':row.Date1,
                                         'Welding':row.Date2,
                                         'PC/Paint':row.Date3,
                                         'Paint Fill':row.Date4,
                                         'Packaging':row.Date5,
                                         'Customer':self.get_customer_name(row.OrderNumber)})
                NumSets += row.QuantityNeeded
                try:
                    dataRow = dataRow.append(
                        pandas.Series({'Ship By':row.ExpectedShipDate.date()}))
                except:
                    pass
                try:
                    dataRow = dataRow.append(
                        pandas.Series({'Ordered':row.DetailDate.date()}))
                except:
                    pass

                dataRow.name = str(row.OrderNumber)+'.'+str(row.ItemNumber).zfill(2)
                report = report.append(dataRow)


        report = report.sort_values('Days')
        cols = ['SKU','Sets','Days','Ordered','Ship By','Status',
                'Engraving','Welding','PC/Paint','Paint Fill','Packaging', 'Customer']
        report = report[cols]

        #Create Inventory Report DataFrame
        invReport = pandas.DataFrame()

        #Write DataFrames to excel sheets
        #Create file and workbook
        writer = pandas.ExcelWriter(path.join(filepath,filename),
                                    engine = 'xlsxwriter',
                                    datetime_format='m/dd hh:mm',
                                    date_format='mm/dd/yy')
        report.to_excel(writer, sheet_name='Status Tracker')

        workbook = writer.book

        #Write Status Report sheet
        worksheet = writer.sheets['Status Tracker']

        alertFormat = workbook.add_format({'bold':True,'font_color':'red','border':1})
        noticeFormat = workbook.add_format({'border':3})

        worksheet.set_column('F:F', 9)
        worksheet.set_column('E:E', 9)
        worksheet.set_column('B:B', 13)
        worksheet.set_column('G:G', 13)
        worksheet.set_column('H:L', 10)
        worksheet.set_column('C:D', 4)
        worksheet.set_column('M:M', 30)

        i=0
        for index,row in report.iterrows():
            i+=1
            try:
                 days = int(row['Days'])
            except TypeError:
                continue

            if days == 0:
                worksheet.write(i,3, days, noticeFormat)
            if days < 0:
                worksheet.write(i,3, days, alertFormat)

            if row['Ship By'] == '' or row['Ship By'] is None:
                worksheet.write(i,5,row['Ship By'],noticeFormat)

            staticDays, columnName, colNum = getStationDays(row)
            if staticDays == 1:
                try: name = row[columnName].strftime('%m/%d %H:%M')
                except AttributeError:
                    name = row[columnName]
                worksheet.write(i,colNum,name, noticeFormat)
            elif staticDays > 1:
                try: name = row[columnName].strftime('%m/%d %H:%M')
                except AttributeError:
                    name = row[columnName]
                worksheet.write(i,colNum,name, alertFormat)

        ###Write Pipeline Summary

        #Create sheet
        summ = pandas.DataFrame()
        summ.to_excel(writer, sheet_name='Summary')
        summary = writer.sheets["Summary"]

        #Count Orders
        NumItems = len(OrderList)
        OrderList = set(OrderList)
        NumOrders = len(OrderList)
        CashFlow = 0

        NumLateItems = len(LateOrderList)
        LateOrderList = set(LateOrderList)
        NumLateOrders = len(LateOrderList)
        LateCashFlow = 0

        #Get Sales totals
        SQL = """
SELECT OrderNumber, ProductTotal
FROM Orders
WHERE OrderNumber = ?"""
        for OrderNum in OrderList:
            try:
                self.cursor.execute(SQL, OrderNum)
                cost = self.cursor.fetchone().ProductTotal
                CashFlow += cost
                if OrderNum in LateOrderList:
                    LateCashFlow += cost
            except:
                print("No items found for order "+str(OrderNum))

        #Calculate Percentages
        PctLateSets = NumLateSets*100/NumSets
        PctLateItems = NumLateItems*100/NumItems
        PctLateOrders = NumLateOrders*100/NumOrders
        PctLateCashFlow = LateCashFlow*100/CashFlow


        summary.write(1,0,"Orders",alertFormat)
        summary.write(2,0,"Items",alertFormat)
        summary.write(3,0,"Sets",alertFormat)
        summary.write(4,0,"Sales",alertFormat)

        summary.write(0,1,"Backordered",alertFormat)
        summary.write(0,2,"Late",alertFormat)
        summary.write(0,3,"Percent Late",alertFormat)

        summary.write(1,1,NumOrders)
        summary.write(2,1,NumItems)
        summary.write(3,1,NumSets)
        summary.write(4,1,'$'+str("%.2f" % CashFlow))

        summary.write(1,2,NumLateOrders)
        summary.write(2,2,NumLateItems)
        summary.write(3,2,NumLateSets)
        summary.write(4,2,'$'+str("%.2f" % LateCashFlow))

        summary.write(1,3,"%.2f" % PctLateOrders)
        summary.write(2,3,"%.2f" % PctLateItems)
        summary.write(3,3,"%.2f" % PctLateSets)
        summary.write(4,3,"%.2f" % PctLateCashFlow)

        summary.set_column('B:D', 20)

        writer.save()
        return(path.join(filepath,filename))

    def getSalesRecord(self, skuList, startDate=None, daysDelta=90, endDate=None):
        if isinstance(skuList, str): skuList = [skuList]
        if isinstance(skuList, int): skuList = [skuList]
        salesDict = {}
        incomeDict = {}
        rankDict = {}
        if startDate is None:
            startDate = datetime.today()

        if endDate is None and daysDelta is not None:
            endDate = startDate - timedelta(days=daysDelta)

        if endDate is None:
            date_params = ()
            SQL = """
SELECT QuantityShipped, QuantityReturned, PricePerUnit, CostPerUnit
FROM "Order Details"
WHERE SKU = ? OR SUBSTRING(SKU, 1, 5) = ?"""

        else:
            date_params = (startDate, endDate)
            SQL = """
SELECT QuantityShipped, QuantityReturned, PricePerUnit, CostPerUnit
FROM "Order Details"
WHERE (SKU = ? OR SUBSTRING(SKU, 1, 5) = ?) AND DetailDate < ? AND DetailDate > ?"""

        total = 0
        for sku in skuList:
            self.cursor.execute(SQL, sku, sku, *date_params)
            quantities = self.cursor.fetchall()

            skuTotal = [0,0,0]
            for quant in quantities:
                netSale = (quant.QuantityShipped - quant.QuantityReturned)
                try:
                     gross = (quant.PricePerUnit) * netSale
                except TypeError:
                     gross = 0
                try:
                    net = (quant.PricePerUnit - quant.CostPerUnit) * netSale
                except TypeError:
                    net = 0

                skuTotal[0] += netSale
                skuTotal[1] += gross
                skuTotal[2] += net
            total += skuTotal[0]
            salesDict[sku] = skuTotal[0]
            incomeDict[sku] = (skuTotal[1], skuTotal[2])

        #Sorting by value using lambda witchcraft
        rankTally = 0
        batchNum = 0
        currentRank = "A"
        rankLevels = {"A":0.25,
                      "B":0.5,
                      "C":0.75,
                      "D":1}
        sortedDict = [(k, salesDict[k]) for k in sorted(salesDict, key=salesDict.get, reverse=True)]
        for sku, num in sortedDict:
            #Computer completely filled with bees
            if batchNum == num:
                rankDict[sku] = currentRank
            else:
                batchNum = num
                for rank in sorted(rankLevels):
                    if rankTally <= (total*rankLevels[rank]):
                        #print(total*rankLevels[rank])
                        rankDict[sku] = rank
                        currentRank = rank
                        break
            rankTally += num
        return(salesDict, rankDict, incomeDict)

    def get_order_details(self):
        SQL = """
SELECT [Order Details].SKU, [Order Details].QuantityShipped, [Order Details].QuantityReturned, [Order Details].PricePerUnit, [Order Details].CostPerUnit, [Order Details].DetailDate
FROM [Order Details] INNER JOIN [Orders] ON ([Order Details].OrderNumber = [Orders].OrderNumber)
WHERE [Order Details].Adjustment = 0 AND [Orders].Approved <> 0
ORDER BY [Order Details].SKU
"""
#AND ([Order Details].QuantityShipped - [Order Details].QuantityReturned) > 0
        self.cursor.execute(SQL)
        data = self.cursor.fetchall()
        return(data)

    def get_item_status(self, orderNum, itemNum):
        SQL = """
SELECT Status
FROM "Order Details"
WHERE Ordernumber = ? AND ItemNumber = ?
"""
        params = (orderNum, itemNum)
        self.cursor.execute(SQL,params)
        data = self.cursor.fetchone()
        if data is None:
            data = ["CANCELLED"]
        return(str(data[0]))

    def order_is_cancelled(self, orderNum):
        SQL = """
SELECT Cancelled
FROM Orders
WHERE Ordernumber = ?
"""
        params = (orderNum)
        self.cursor.execute(SQL,params)
        data = self.cursor.fetchone()
        if data is None:
            data = [True]
        return(data[0])

    def set_primary_image(self, sku, imageURL):
        SQL = """
UPDATE Inventory
SET Image = ?
WHERE LocalSKU = ?
"""
        self.cursor.execute(SQL, imageURL, sku)
        self.conn.commit()
        return

    def set_secondary_image(self, sku, imageURL):
        SQL = """
UPDATE Inventory
SET Text5 = ?
WHERE LocalSKU = ?
"""
        self.cursor.execute(SQL, imageURL, sku)
        self.conn.commit()
        return

    def get_sold_skus(self):
        SQL = """
SELECT DISTINCT SKU
FROM [Order Details]
WHERE Adjustment = 0 AND (QuantityShipped - QuantityReturned) > 0
"""
        self.cursor.execute(SQL)
        data = self.cursor.fetchall()
        return(data)

    def getOrderTotals(self):
        startDate = date(2013,1,1)
        SQL = """
SELECT Customers.Company, Customers.PriceLevel, Customers.Text5 AS IncomeStream, [Order Details].OrderNumber, Orders.ProductTotal, Orders.Discount, Sum(Orders.ShippingTotal) AS ShippingTotal, Orders.FinalProductTotal, Orders.RevisedDiscount, Sum(Orders.FinalShippingTotal) AS FinalShippingTotal, Sum([Order Details].QuantityShipped) AS QuantityShipped, Sum([Order Details].QuantityReturned) AS QuantityReturned, Orders.OrderDate
FROM ((Orders INNER JOIN [Order Details] ON Orders.OrderNumber = [Order Details].OrderNumber) INNER JOIN Inventory ON [Order Details].SKU = Inventory.LocalSKU) INNER JOIN Customers ON Orders.CustomerID = Customers.CustomerID
WHERE (Orders.OrderDate>=? AND [Order Details].Adjustment=?) AND (Inventory.Category=? OR Inventory.Category=? OR Inventory.Category=? OR Inventory.Category=?)
GROUP BY Customers.Company, Customers.PriceLevel, Customers.Text5, [Order Details].OrderNumber, Orders.ProductTotal, Orders.Discount, Orders.FinalProductTotal, Orders.RevisedDiscount, Orders.OrderDate;
"""
        params = (startDate, False, 'FGPN', 'Base', 'Private Label', 'MTO')
        self.cursor.execute(SQL, params)
        data = self.cursor.fetchall()
        return(data)

    def getCustomerData(self):
        SQL = """
SELECT Customers.Company, TempPriceData.Level AS [PriceLevel], Customers.Text5 AS [IncomeStream], Sum(qryOrderProductQuantity.ProductTotal) AS [Gross Sale], Sum(qryOrderProductQuantity.Discount) AS [Gross Discount], Sum(qryOrderProductQuantity.SumOfShippingTotal) AS [Gross Shipping], Sum(qryOrderProductQuantity.FinalProductTotal) AS [Net Sale], Sum(qryOrderProductQuantity.RevisedDiscount) AS [Net Discount], Sum(qryOrderProductQuantity.SumOfFinalShippingTotal) AS [Net Shipping], Sum(qryOrderProductQuantity.SumOfQuantityShipped) AS QuantityShipped, Sum(qryOrderProductQuantity.SumOfQuantityReturned) AS QuantityReturned
FROM (Customers LEFT JOIN TempPriceData ON Customers.PriceLevel = TempPriceData.PriceLevel) RIGHT JOIN qryOrderProductQuantity ON Customers.CustomerID = qryOrderProductQuantity.CustomerID
WHERE (((qryOrderProductQuantity.OrderDate)>=#1/1/2013#))
GROUP BY Customers.Company, TempPriceData.Level, Customers.Text5
HAVING (((Sum(qryOrderProductQuantity.SumOfQuantityShipped))>0));
"""
        self.cursor.execute(SQL)
        data = self.cursor.fetchall()
        return(data)

    def getCustomerOrderItems(self, startTime, endTime):
        SQL = """
SELECT Customers.PriceLevel, Orders.OrderDate, [Order Details].SKU, [Order Details].PricePerUnit, Customers.CustomerID, [Order Details].OrderNumber
FROM (Orders INNER JOIN Customers ON Orders.CustomerID = Customers.CustomerID) INNER JOIN [Order Details] ON Orders.OrderNumber = [Order Details].OrderNumber
WHERE (Customers.PriceLevel>0 AND Orders.OrderDate<=? AND Orders.OrderDate>? AND (([Order Details].Adjustment)=0));
"""
        params = (startTime, endTime)
        self.cursor.execute(SQL, params)
        data = self.cursor.fetchall()
        return(data)

if __name__ == '__main__':

    with Database('BOT') as db:
        print(db.order_is_cancelled("1001"))
        print('"{0}"'.format(db.get_item_status("1001","02")))
        input("")
        print('Databaseutils')
